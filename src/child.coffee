Q = require 'q'
Connection = require 'q-connection'

workerFile = process.argv[2]
workerMod = require(workerFile)

Connection process,
	map: (arrayData, methodName, extraArgs) ->
		Q.all arrayData.map (data) ->
			timestamp = new Date()
			workerMod[methodName](data, extraArgs...).then (result) ->
				{
					elapsed: new Date() - timestamp,
					result
				}
