Q = require 'q'
Connection = require 'q-connection'

workerFile = process.argv[2]
workerMod = require(workerFile)

Connection process,
	map: (method, array) ->
		Q.all(array.map workerMod[method])
