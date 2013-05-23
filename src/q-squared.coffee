Q = require('q')
Queue = require('q/queue')
Connection = require('q-connection')
Qx = require('qx')
os = require('os')
fork = require('child_process').fork

Q.longStackJumpLimit = 0;

_extend = (baseObject, extendWith...) ->
  for object in extendWith
    for key,val of object
      if val instanceof Object
        baseObject[key] = _extend(baseObject[key] ? {}, val)
      else baseObject[key] = val
  baseObject

class qSquared extends Qx
    @defaults:
        concurrent: os.cpus().length
    initialize: (@filePath, options) ->
        @options = _extend({}, qSquared.defaults, options)
        @workerQueue = Queue()
        @workers = for n in [1..@options.concurrent]
            worker = new Worker(@filePath,options)
            @workerQueue.put(worker)
            worker

    map: (array, methodName) ->
        return super array, (arg) =>
            @workerQueue.get()
                .then (worker) =>
                    [worker, worker.invoke(methodName, arg)]
                .spread (worker, result) =>
                    @workerQueue.put(worker)
                    result

class Worker extends Connection
    initialize: (@filePath, options) ->
        @proc = fork(@filePath,options)
        super @proc

class qSquared.Child
    initialize: (args...) ->
        Connection(process, args...)

modules.exports = qSquared
