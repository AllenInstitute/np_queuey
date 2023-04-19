# np_queuey
Tools for submitting and processing jobs through a message queue for Mindscope Neuropixels workflows.


- producers (code that submits tasks) should not require the environment
  for running tasks: just `np_queuey`, the name of a function to schedule, and
  any input args

- consumers must be configured to run in an environment that *does* have any
  required dependencies, beyond those provided by `np_queuey`
  

## todo 
- setup behavior session upload task
    - path to modules in 
- make __main__ cli and executable