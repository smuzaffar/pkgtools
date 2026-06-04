import sys
from queue import Queue, PriorityQueue, Empty
from io import StringIO
from threading import Thread
from time import sleep
import threading
import traceback
from rmanager import ResourceManager

from enum import Enum

class State(Enum):
    UNKNOWN = 0
    PENDING = 1
    RUNNING = 3
    DONE = 4
    BROKEN = 5

# Helper class to avoid conflict between result
# codes and quit state transition.
class _SchedulerQuitCommand(object):
  pass

class Scheduler(object):
  # A simple job scheduler.
  # Workers queue is to specify to threads what to do. Results
  # queue is whatched by the master thread to wait for results
  # of workers computations.
  # All worker threads begin trying to fetch from the command queue (and are
  # therefore blocked).
  # Master thread does the scheduling and then sits waiting for results.
  # Scheduling implies iterating on the list of jobs and creates an entry
  # in the parallel queue for all the jobs which does not have any dependency
  # which is not done.
  # If a job has dependencies which did not build, move it do the failed queue.
  # Post an appropriate build command for all the new jobs which got added.
  # If there are jobs still to be done, post a reschedule job on the command queue
  # if there are no jobs left, post the "kill worker" task.
  def __init__(self, parallelThreads, logDelegate=None, buildStats=None, parallelDownloads=2):
    self.cv = threading.Condition()
    self.activeTasks = 0
    self.shutdownRequested = False
    self.workersQueue = PriorityQueue()
    self.resultsQueue = Queue()
    self.notifyQueue = Queue()
    self.readyQueue = Queue()
    self.parallelReady = {}
    self.serialEnqueued = set()
    self.jobs = {}
    self.reverseDeps = {}
    self.stateCounter = {}
    for state in State:
      self.stateCounter[state] = 0
    self.doneJobs = set()
    self.doneOrdered = []
    self.brokenJobs = set()
    self.brokenOrdered = []
    self.parallelThreads = parallelThreads+parallelDownloads
    self.logDelegate = logDelegate
    self.resourceManager = None
    self.runningJobsCount = {"build": 0, "fetch": 0, "download": 0, "force": 0,"max_build": parallelThreads, "max_download": parallelDownloads}
    self.errors = {}
    self.workers = []
    self.masterThread = None
    self.final_job = "final-job"
    if not logDelegate:
      self.logDelegate = self.__doLog 
    if buildStats:
      self.resourceManager = ResourceManager(buildStats, self)

  def set_state(self, taskId, new_state):
    assert(self.masterThread == threading.current_thread())
    old = self.jobs[taskId]["state"]
    assert(old != new_state)
    if old in (State.DONE, State.BROKEN):
        return
    self.jobs[taskId]["state"] = new_state
    self.stateCounter[old] -= 1
    self.stateCounter[new_state] += 1
    if new_state == State.BROKEN:
      self.brokenOrdered.append(taskId)
      self.brokenJobs.add(taskId)
    elif new_state == State.DONE:
      self.doneOrdered.append(taskId)
      self.doneJobs.add(taskId)
    return

  def run(self):
    self.masterThread = threading.current_thread()
    for i in range(self.parallelThreads):
      t = Thread(target=self.__workerLoop)
      self.workers.append(t)
      t.start()
    while True:
      self.__doNotifications()
      try:
        who, item = self.resultsQueue.get(timeout=0.1)
        item[0](*item[1:])
      except Empty:
        pass
      except KeyboardInterrupt:
        print("Ctrl-C received, shutting down")
        self.__requestShutdown()
      with self.cv:
        if self.shutdownRequested:
          break
        if self.__isQuiescent():
          self.__requestShutdown()
    self.__doNotifications()
    for t in self.workers:
      t.join()
    self.jobs[self.final_job] = {"scheduler": "serial", "deps": list(self.jobs.keys()), "spec": None, "state": State.RUNNING}
    self.stateCounter[self.jobs[self.final_job]["state"]] += 1
    if self.stateCounter[State.BROKEN]:
      self.set_state(self.final_job, State.BROKEN)
    else:
      self.set_state(self.final_job, State.DONE)
    return

  def __workerLoop(self):
    while True:
      pri, taskId, item = self.workersQueue.get()
      if taskId == "__QUIT__": return
      with self.cv:
        self.activeTasks += 1
      try:
        result = item[0](*item[1:])
      except Exception as e:
        s = StringIO()
        traceback.print_exc(file=s)
        result = s.getvalue()
      if not self.__workerDone(taskId, result, item):
        return

  def __requestShutdown(self):
    with self.cv:
      if self.shutdownRequested:
        return
      self.shutdownRequested = True
    for _ in self.workers:
        self.workersQueue.put((1, "__QUIT__", None))

  def __workerDone(self, taskId, result, item):
    with self.cv:
      self.activeTasks -= 1
      self.cv.notify_all()
    if self.resourceManager and taskId.startswith('build-'):
      self.notifyMaster(self.resourceManager.releaseResourcesForExternal, taskId)
    if isinstance(result, _SchedulerQuitCommand):
      self.notifyTaskMaster(self.__releaseWorker)
      return False
    if result:
      self.log(str(item) + " failed.\n"+result)
    else:
      self.log(str(item) + " done")
    self.notifyTaskMaster(self.__updateJobStatus, taskId, result, True)
    return True

  def __isQuiescent(self):
    return (
        self.activeTasks == 0 and
        self.stateCounter[State.PENDING] == 0 and
        self.stateCounter[State.RUNNING] == 0 and
        self.notifyQueue.empty() and
        self.resultsQueue.empty()
    )

  def __doNotifications(self):
    while True:
      try:
        who, item = self.notifyQueue.get_nowait()
        item[0](*item[1:])
      except Empty:
        break

  def __releaseWorker(self):
    self.parallelThreads -= 1

  def __tryActivate(self, taskId):
    job = self.jobs[taskId]
    if job["state"] != State.PENDING:
        return
    if job.get("queued", False):
        return
    for d in job["deps"]:
      if d not in self.jobs:
        return
      dep_state = self.jobs[d]["state"]
      if dep_state == State.BROKEN:
        self.__failJob(taskId, f"dep {d} failed")
        return
      if dep_state != State.DONE:
        return
    job["queued"] = True
    self.readyQueue.put(taskId)
    self.notifyMaster(self.__dispatchReadyJobs)

  def __failJob(self, taskId, reason):
    job  = self.jobs.get(taskId)
    if not job:
      return
    if job["state"] in (State.DONE, State.BROKEN):
      return
    self.set_state(taskId, State.BROKEN)
    self.errors[taskId] = reason
    for child in self.reverseDeps.get(taskId, []):
        self.__failJob(child, f"dependency {taskId} failed")
    return

  def __addPending(self, taskId):
    job = self.jobs[taskId]
    # 1. register reverse dependency edges (idempotent)
    for dep in job["deps"]:
        self.reverseDeps.setdefault(dep, set()).add(taskId)
    # 2. state bookkeeping (only once)
    self.stateCounter[State.PENDING] += 1
    job["state"] = State.PENDING
    job["queued"] = False
    # 3. jobs try activation
    # (important: serial/parallel should behave uniformly here)
    self.__tryActivate(taskId)

  def parallel(self, taskId, deps, *spec):
    if threading.current_thread() is not self.masterThread:
      self.notifyMaster(
        self.__parallelImpl,
        taskId,
        deps,
        *spec
      )
      return
    self.__parallelImpl(taskId, deps, *spec)

  def __parallelImpl(self, taskId, deps, *spec):
    if taskId in self.jobs: return
    self.jobs[taskId] = {"scheduler": "parallel", "deps": deps, "spec":spec, "priorty": 1, "task_type": "force"}
    task_types = taskId.split("-")
    if (len(task_types)>1) and (task_types[0] in ["build", "download", "fetch"]):
      self.jobs[taskId]["task_type"] = task_types[0]
      try:
        self.jobs[taskId]["priorty"] = 100000-spec[1].requiredBy
      except:
        self.jobs[taskId]["priorty"] = 1
    self.__addPending(taskId)

  def __dispatchReadyJobs(self):
    # 1. Drain readyQueue into a local batch
    ready = []
    while True:
      try:
        taskId = self.readyQueue.get_nowait()
        ready.append(taskId)
      except Empty:
         break
    if not ready:
        return
    for taskId in ready:
      job = self.jobs[taskId]
      if job["state"] != State.PENDING:
        continue
      if job["scheduler"] == "serial":
        self.set_state(taskId, State.RUNNING)
        self.resultsQueue.put((threading.current_thread(), job["spec"]))
      else:
        self.parallelReady[taskId] = job

    if not self.parallelReady:
      return

    buildJobs =[]
    downloadJobs = []
    forceJobs = []
    bldCount = self.runningJobsCount["max_build"]-self.runningJobsCount["build"]
    dwnCount = self.runningJobsCount["max_download"]-self.runningJobsCount["download"]
    for taskId, job in sorted(self.parallelReady.items(), key=lambda x: x[1].get('priorty',1)):
      taskType = job["task_type"]
      if taskType == "download":
        if dwnCount>0:
          downloadJobs.append(taskId)
          dwnCount -= 1
      elif taskType == "build":
        if bldCount>0: #include all build jobs so that we can match those which can be run
          buildJobs.append(taskId)
      else:
        forceJobs.append(taskId)
    if bldCount>0 and buildJobs:
      if self.resourceManager:
        buildJobs = self.resourceManager.allocResourcesForExternals(buildJobs, count=bldCount)
      else:
        buildJobs = buildJobs[:bldCount]
    for taskId in forceJobs + downloadJobs + buildJobs:
      self.parallelReady.pop(taskId, None)
      self.set_state(taskId, State.RUNNING)
      self.runningJobsCount[self.jobs[taskId]["task_type"]] += 1
      self.__scheduleParallel(taskId, self.jobs[taskId]["spec"], priorty=self.jobs[taskId]["priorty"])

  # Update the job with the result of running.
  def __updateJobStatus(self, taskId, error, parallel = True):
    if parallel:
      self.runningJobsCount[self.jobs[taskId]["task_type"]] -= 1
    else:
      self.serialEnqueued.discard(taskId)
    if not error:
      self.set_state(taskId, State.DONE)
    else:
      self.set_state(taskId, State.BROKEN)
      self.errors[taskId] = error
    for depTask in self.reverseDeps.get(taskId, set()):
      if self.jobs[depTask]["state"] != State.PENDING:
        continue
      self.__tryActivate(depTask)
    self.reverseDeps.pop(taskId, None)
    self.notifyMaster(self.__dispatchReadyJobs)
  
  # One task at the time.
  def __scheduleParallel(self, taskId, commandSpec, priorty=1):
    self.workersQueue.put((priorty, taskId, commandSpec))

  # Helper to enqueue commands for all the threads.
  def shout(self, *commandSpec):
    for x in range(self.parallelThreads):
      self.__scheduleParallel("quit-" + str(x), commandSpec)

  # Helper to enqueu replies to the master thread.
  def notifyTaskMaster(self, *commandSpec):
    self.resultsQueue.put((threading.currentThread(), commandSpec))

  def notifyMaster(self, *commandSpec):
    self.notifyQueue.put((threading.currentThread(), commandSpec))

  def forceDone(self, taskId):
    if threading.current_thread() is not self.masterThread:
      self.notifyMaster(self.__forceDoneImpl,taskId)
      return
    self.__forceDoneImpl(taskId, deps, *commandSpec)

  def __forceDoneImpl(self, taskId):
    if not taskId in self.jobs:
      self.jobs[taskId]={"scheduler": "serial", "state": State.PENDING}
      self.stateCounter[State.PENDING] +=1
    if self.jobs[taskId]["state"] in [State.DONE, State.BROKEN]: return
    parallel = (self.jobs[taskId]["scheduler"] == "parallel")
    self.set_state(taskId, State.RUNNING)
    self.__updateJobStatus(taskId, "", parallel)

  def serial(self, taskId, deps, *commandSpec):
    if threading.current_thread() is not self.masterThread:
      self.notifyMaster(
        self.__serialImpl,
        taskId,
        deps,
        *commandSpec
      )
      return
    self.__serialImpl(taskId, deps, *commandSpec)

  def __serialImpl(self, taskId, deps, *commandSpec):
    if taskId in self.jobs: return
    spec = [self.doSerial, taskId, deps] + list(commandSpec)
    self.jobs[taskId] = {"scheduler": "serial", "deps": deps, "spec": spec}
    self.__addPending(taskId)

  def doSerial(self, taskId, deps, *commandSpec):
    brokenDeps = [dep for dep in deps if self.jobs[dep]["state"] == State.BROKEN]
    print("HERE",taskId, self.jobs[taskId])
    #self.set_state(taskId, State.RUNNING)
    if brokenDeps:
      error = "The following dependencies could not complete:\n%s" % "\n".join(brokenDeps)
      self.__updateJobStatus(taskId, error, parallel = False)
      return
    result = ""
    try:
      result = commandSpec[0](*commandSpec[1:])
    except Exception as e:
      s = StringIO()
      traceback.print_exc(file=s)
      result = s.getvalue()
    self.__updateJobStatus(taskId, result, parallel = False)
 
  # Helper method to do logging:
  def log(self, s, level=0):
    self.notifyMaster(self.logDelegate, s, level)

  # Task which forces a worker to quit.
  def quit(self):
    self.log("Requested to quit.")
    return _SchedulerQuitCommand()

  # Helper for printouts.
  def __doLog(self, s, level=0):
    print (s)

  def reschedule(self):
    pass

def dummyTask():
  sleep(0.1)

def dummyTaskLong():
  sleep(1)

def errorTask():
  return "This will always have an error"

def exceptionTask():
  raise Exception("foo")

# Mimics cmsBuild workflow.
def scheduleMore(scheduler):
  scheduler.parallel("download-file", [], dummyTask)
  scheduler.parallel("build-test", ["download-file"], dummyTask)
  scheduler.serial("install", ["build-test"], dummyTask)

def run_test(scheduler, skip_run=False):
  if not skip_run:
    scheduler.run()
  assert(len(scheduler.serialEnqueued)==0)
  if scheduler.stateCounter[State.BROKEN]:
    assert(len(scheduler.brokenOrdered)>=1)
    assert(scheduler.brokenOrdered[-1] == scheduler.final_job)
  elif scheduler.stateCounter[State.DONE]:
    assert(len(scheduler.doneOrdered)>=1)
    assert(scheduler.doneOrdered[-1] == scheduler.final_job)
  assert(scheduler.stateCounter[State.BROKEN]+scheduler.stateCounter[State.DONE] ==
         len(scheduler.brokenOrdered)+len(scheduler.doneOrdered))
  for state in [State.UNKNOWN, State.PENDING, State.RUNNING]:
    if scheduler.stateCounter[state] != 0:
      print(scheduler.stateCounter)
      all_jobs = list(scheduler.jobs.keys())
      print("Total Jobs:", len(all_jobs))
      for j in scheduler.jobs:
        if scheduler.jobs[j]["state"] == State.PENDING:
          print("JOB  %s %s %s" % (j , scheduler.jobs[j]["scheduler"], scheduler.jobs[j]["state"]))
          for dep in scheduler.jobs[j]["deps"]:
            print("  DEP: %s %s %s" % (dep, scheduler.jobs[j]["scheduler"], scheduler.jobs[dep]["state"]))
    assert(scheduler.stateCounter[state]==0) 
  for item in scheduler.runningJobsCount.keys():
    if item.startswith("max_"):
      continue
    assert(scheduler.runningJobsCount[item]==0)
  for task_id, job in scheduler.jobs.items():
    for dep in job.get("deps", []):
        if scheduler.jobs[dep]["state"] == State.DONE:
            assert(dep in scheduler.doneJobs)
            assert(dep in scheduler.doneOrdered)
  for task_id in scheduler.doneJobs:
    #if task_id == scheduler.final_job:
    #    continue
    for dep in scheduler.jobs[task_id]["deps"]:
        assert(dep in scheduler.doneJobs)
        assert(dep in scheduler.doneOrdered)
  all_jobs = set(scheduler.jobs.keys())
  assert all(
    scheduler.jobs[j]["state"] in (State.DONE, State.BROKEN)
    for j in all_jobs
  )
  if not scheduler.stateCounter[State.BROKEN]:
    print("HERE:",scheduler.reverseDeps)
    assert(len(scheduler.reverseDeps)==0)

if __name__ == "__main__":
  from test_scheduler import RandomSchedulerTest
  scheduler = Scheduler(10)
  test = RandomSchedulerTest(
    scheduler,
    initial_jobs=500,
    max_deps=10,
    dynamic_job_probability=0.2,
    serial_probability=0.25,
    failure_probability=0.01,
    seed=12345
  )
  test.run()
  print("Done:", scheduler.stateCounter[State.DONE])
  print("Broken:", scheduler.stateCounter[State.BROKEN])
  run_test(scheduler, True)
  exit(0)

  scheduler = Scheduler(10)
  run_test(scheduler)

  scheduler = Scheduler(1)
  run_test(scheduler)
  
  scheduler = Scheduler(10)
  scheduler.parallel("test", [], scheduler.log, "This is england");
  run_test(scheduler)

  scheduler = Scheduler(1)
  for x in range(10):
    scheduler.parallel("test", [], dummyTask)
    scheduler.serial("test", [], dummyTask)
  run_test(scheduler)
  assert(scheduler.stateCounter[State.BROKEN] == 0)
  assert(len(scheduler.jobs) == 2)

  scheduler = Scheduler(10)
  for x in range(50):
    scheduler.parallel("test" + str(x), [], dummyTask)
  run_test(scheduler)
  assert(scheduler.stateCounter[State.BROKEN] == 0)
  assert(len(scheduler.jobs) == 51)

  scheduler = Scheduler(1)
  scheduler.parallel("test", [], errorTask)
  run_test(scheduler)
  # Again, since the toplevel one always depend on all the others
  # it is always broken if something else is brokend.
  assert(scheduler.stateCounter[State.BROKEN] == 2)
  assert(scheduler.stateCounter[State.DONE] == 0)

  # Check dependency actually works.
  scheduler = Scheduler(10)
  scheduler.parallel("test2", ["test1"], dummyTask)
  scheduler.parallel("test1", [], dummyTaskLong) 
  run_test(scheduler)
  assert(scheduler.doneOrdered == ["test1", "test2", scheduler.final_job])

  # Check dependency actually works.
  scheduler = Scheduler(10)
  scheduler.parallel("build-test3", ["build-test2"], dummyTask)
  scheduler.parallel("build-test2", ["build-test1"], errorTask)
  scheduler.parallel("build-test1", [], dummyTaskLong) 
  run_test(scheduler)
  assert(scheduler.doneOrdered == ["build-test1"])
  assert(scheduler.brokenOrdered == ["build-test2", "build-test3", scheduler.final_job])

  # Check ctrl-C will exit properly.
  scheduler = Scheduler(2)
  doneOrdered = ["build-test" + str(x) for x in range(250)]
  for x in doneOrdered:
    scheduler.parallel(x, [], dummyTask)
  print ("Print Control-C to continue")
  run_test(scheduler)
  doneOrdered.append(scheduler.final_job)
  assert(scheduler.stateCounter[State.DONE] == len(doneOrdered))
  assert(scheduler.doneJobs == set(doneOrdered))

  scheduler = Scheduler(16)
  for x in range(250):
    scheduler.parallel("test" + str(x), [], dummyTask)
  print ("Print Control-C to continue")
  run_test(scheduler)

  scheduler = Scheduler(2)
  doneOrdered = ["test" + str(x) for x in range(250)]
  for x in doneOrdered:
    scheduler.serial(x, [], dummyTask)
  run_test(scheduler)
  doneOrdered.append(scheduler.final_job)
  assert(scheduler.doneOrdered == doneOrdered)
  assert(scheduler.stateCounter[State.DONE] == len(doneOrdered))
  assert(scheduler.doneJobs == set(doneOrdered))

  # Handle tasks with exceptions.
  scheduler = Scheduler(2)
  scheduler.parallel("build-test", [], exceptionTask)
  run_test(scheduler)
  assert(scheduler.errors["build-test"])

  # Handle tasks which depend on tasks with exceptions.
  scheduler = Scheduler(2)
  scheduler.parallel("build-test0", [], dummyTask)
  scheduler.parallel("build-test1", [], exceptionTask)
  scheduler.parallel("build-test2", ["build-test1"], dummyTask)
  run_test(scheduler)
  assert(scheduler.errors["build-test1"])
  assert(scheduler.errors["build-test2"])

  # Handle serial execution tasks.
  scheduler = Scheduler(2)
  scheduler.serial("test0", [], dummyTask)
  run_test(scheduler)
  assert(scheduler.doneOrdered == ["test0", scheduler.final_job])

  # Handle serial execution tasks, one depends from
  # the previous one.
  scheduler = Scheduler(2)
  scheduler.serial("test0", [], dummyTask)
  scheduler.serial("test1", ["test0"], dummyTask)
  run_test(scheduler)
  assert(scheduler.doneOrdered == ["test0", "test1", scheduler.final_job])

  # Serial tasks depending on one another.
  scheduler = Scheduler(2)
  scheduler.serial("test1", ["test0"], dummyTask)
  scheduler.serial("test0", [], dummyTask)
  run_test(scheduler)
  assert(scheduler.doneOrdered == ["test0", "test1", scheduler.final_job])

  # Serial and parallel tasks being scheduled at the same time.
  scheduler = Scheduler(2)
  scheduler.serial("test1", ["test0"], dummyTask)
  scheduler.serial("test0", [], dummyTask)
  scheduler.parallel("build-test2", [], dummyTask)
  scheduler.parallel("build-test3", [], dummyTask)
  run_test(scheduler)
  scheduler.doneOrdered.sort()
  doneOrdered = ["build-test2", "build-test3", scheduler.final_job, "test0", "test1"]
  assert(scheduler.doneOrdered == doneOrdered)
  assert(scheduler.stateCounter[State.DONE] == len(doneOrdered))

  # Serial and parallel tasks. Parallel depends on serial.
  scheduler = Scheduler(2)
  scheduler.serial("test1", ["test0"], dummyTask)
  scheduler.serial("test0", [], dummyTask)
  scheduler.parallel("build-test2", ["test1"], dummyTask)
  scheduler.parallel("build-test3", ["build-test2"], dummyTask)
  run_test(scheduler)
  assert(scheduler.doneOrdered == ["test0", "test1", "build-test2", "build-test3", scheduler.final_job])

  # Serial task scheduling two parallel task and another dependent
  # serial task. This is actually what needs to be done for building 
  # packages. I.e.
  # The first serial task is responsible for checking if a package is already there,
  # then it queues a parallel download sources task, a subsequent build sources
  # one and finally the install built package one.
  scheduler = Scheduler(3)
  scheduler.serial("check-pkg", [], scheduleMore, scheduler)
  run_test(scheduler)
  assert(scheduler.doneOrdered == ["check-pkg", "download-file", "build-test", "install", scheduler.final_job])
