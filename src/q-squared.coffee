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
      baseObject[key] = val
  baseObject

class qSquared
    @defaults:
        concurrent: os.cpus().length
    constructor: (@filePath, options) ->
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

class Worker
    constructor: (@filePath, options) ->
        @proc = fork(@filePath,options)
        @conn = Connection(@proc)
    invoke: (method, args) ->
        Qx.map args, (arg) ->
            @conn.invoke(method, arg)

qSquared.Child = (args...) ->
        Connection(process, args...)


module.exports = qSquared
