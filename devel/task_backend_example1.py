#!/usr/bin/env python3

import kachery_client as kc
import hither2 as hi

@kc.taskfunction('example1a.2')
def example1a(*, a: str):
    return f'a is {a}'

@hi.function('example1b', '1')
def example1b(*, b: str):
    return f'b is {b}'

@kc.taskfunction('example1b.1')
def example1b_task(*, b: str):
    job = hi.Job(example1b, {'b': b})
    return job

def main():
    kc.run_task_backend(
        channels=['ccm'],
        task_function_ids=[
            'example1a.2',
            'example1b.1'
        ]
    )

if __name__ == '__main__':
    main()