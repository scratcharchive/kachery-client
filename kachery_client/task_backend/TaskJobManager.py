from typing import Any, Dict, List
from .RequestedTask import RequestedTask

class TaskJob:
    def __init__(self, *, requested_task: RequestedTask, job: Any) -> None:
        import hither2 as hi
        if not isinstance(job, hi.Job):
            raise Exception('Not a hither job.')
        self._requested_task = requested_task
        self._job = job
    @property
    def requested_task(self):
        return self._requested_task
    @property
    def job(self):
        return self._job

class TaskJobManager:
    def __init__(self):
        self._task_jobs: Dict[str, TaskJob] = {}
    def add_task_job(self, *, requested_task: RequestedTask, job: Any):
        import hither2 as hi # Only import hither if we have a hither job
        if not isinstance(job, hi.Job):
            raise Exception('Not a hither job.')
        if requested_task.task_id in self._task_jobs:
            raise Exception('Unexpected. Already have job with task hash.')
        self._task_jobs[requested_task.task_id] = TaskJob(requested_task=requested_task, job=job)
    def get_existing_job_for_task(self, requested_task: RequestedTask):
        if requested_task.task_id in self._task_jobs:
            return self._task_jobs[requested_task.task_id].job
        else:
            return None
    def process_events(self):
        task_jobs_to_delete: List[str] = []
        has_jobs = False
        for k, v in self._task_jobs.items():
            has_jobs = True
            job = v.job
            requested_task = v.requested_task
            if job.status != requested_task.status:
                if job.status == 'error':
                    error_message = str(job.result.error)
                else:
                    error_message = None
                if job.status == 'finished':
                    result = job.result.return_value
                else:
                    result = None
                requested_task.update_status(status=job.status, error_message=error_message, result=result)
            if requested_task.status in ['error', 'finished']:
                task_jobs_to_delete.append(k)
        for k in task_jobs_to_delete:
            del self._task_jobs[k]
        if has_jobs:
            import hither2 as hi # Only import hither if we have a hither job
            hi.wait(0)
            