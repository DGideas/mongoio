#!/usr/bin/env python3
__author__ = "dgideas@outlook.com"
__version__ = "1.0"
import sys
import json
try:
	import pymongo
except:
	print(sys.argv[0] + "need pymongo, please install pymongo first.")
	print("For example, try to execute:\n\tpip3 install pymongo")
	sys.exit(1)

mongoActionExportFlag = "EXPORT"
mongoActionImportFlag = "IMPORT"

config = {
	"host": "127.0.0.1",
	"port": "27017",
	"db": "test",
	"collection": "test",
	"action": mongoActionExportFlag,
	"filename": "mongoio.json",
	"noconfirm": False
}

configType = {
	conf: type(config[conf]) for conf in config
}

configureNameAlt = {
	"-h": "host",
	"-p": "port",
	"-d": "db",
	"-c": "collection",
	"--coll": "collection",
	"-Y": "noconfirm"
}

def showHelpMsg(_valueLint = "Default"):
	ignoreItem = ("action", "filename")
	
	print("DGideas MongoDB Data Importer/Exporter")
	print(__version__, __author__)
	print("Usage:")
	print("\t", sys.argv[0],
		"[import|export|input|output|dump] <flags> filename[.json]\n")
	for configureKey in config:
		if configureKey not in ignoreItem:
			print("\t", end="")
			for configureNameAltKey in configureNameAlt:
				if configureNameAlt[configureNameAltKey] == configureKey:
					print(configureNameAltKey+", ", end="")
			print("--"+configureKey+"\t"+_valueLint+": "+
				str(config[configureKey]))
	print("\t-H, --help\tShow this help message")

def parseAction(_config, _argv):
	try:
		_argv[1]
	except IndexError:
		showHelpMsg()
		sys.exit(0)
	
	argExportStartswithFlag = ("ex", "out", "dum")
	argImportStartswithFlag = ("im", "in")
	
	actionArg = _argv[1].lower()
	
	if actionArg.startswith(argExportStartswithFlag):
		_config["action"] = mongoActionExportFlag
	elif actionArg.startswith(argImportStartswithFlag):
		_config["action"] = mongoActionImportFlag
	else:
		print("Arguments error: the first arguemnt must be the action\n")
		showHelpMsg()
		sys.exit(0)

def parseArgs(_config, _argv):
	argStartFlag = ("-", "--")
	helpMsgFlag = ("-H", "--help")
	recvFilenameFlag = False
	
	for _arg in _argv:
		if _arg.startswith(helpMsgFlag):
			showHelpMsg()
			sys.exit(0)
	
	for _arg in _argv:
		changedConfigName = None
		if _arg.startswith(argStartFlag):
			for prefix in configureNameAlt:
				if _arg.startswith(prefix):
					changedConfigName = configureNameAlt[prefix]
					break
			if changedConfigName is None:
				arg = _arg.replace("-", "").split("=")
				try:
					changedConfigName = arg[0]
				except KeyError:
					print("No such configure key: " + arg[0])
					showHelpMsg("Current")
					sys.exit(0)
			if len(_arg.split("=")) == 1:
				_config[changedConfigName] = True
			else:
				_config[changedConfigName] = _arg.replace("-", "").split("=")[1]
		else:
			if not recvFilenameFlag:
				_config["filename"] = _arg
				recvFilenameFlag = True
			else:
				print("Dumplicate filename instruction")
				showHelpMsg("Current")
				sys.exit(0)

def confirmOperation(_config):
	print("You are in", _config["host"]+":"+_config["port"]+"@"+_config["db"]+
		"/"+_config["collection"])
	print(_config["action"], "data", 
		["from" if _config["action"] == mongoActionImportFlag else "to"][0], 
		_config["filename"])
	try:
		confirm = input("Continue? [ENTER or N or Ctrl+C] ")
	except KeyboardInterrupt:
		print("\nUser cancelled operation, Abort.")
		sys.exit(0)
	if confirm.lower().startswith("n"):
		print("User cancelled operation, Abort.")
		sys.exit(0)

def varifyConfigure(_config):
	for configureEle in _config:
		if type(_config[configureEle]) == str and \
			configType[configureEle] == bool:
			if _config[configureEle].lower() == "true":
				_config[configureEle] = True
			elif _config[configureEle].lower() == "false":
				_config[configureEle] = False
		if type(_config[configureEle]) != configType[configureEle]:
			print("The config key", configureEle+"'s type is",
				str(type(_config[configureEle]))+", but expected",
				str(configType[configureEle]))
			print("Using -H or --help to display usage message.")
			sys.exit(1)

def _mongoExport(_host, _port, _db, _collection, _filename):
	print("\nConnecting with", _host, "...")
	myclient = pymongo.MongoClient("mongodb://" + _host + ":" + _port + "/")
	mydb = myclient[_db]
	myColl = mydb[_collection]
	print("\tHost connected.")
	
	idList = []
	print("Fetching _id list", end="")
	fetchingCnt = 0
	for document in myColl.find({}, {"_id": 1}):
		idList.append(document["_id"])
		fetchingCnt += 1
		if fetchingCnt >= 10000:
			print(".", end="", flush=True)
			fetchingCnt = 0
	print("\n\t_id list finish fetching. Total " + str(len(idList)))
	
	print("Exporting data from", _db, "/", _collection)
	count = 0
	total = len(idList)
	with open(_filename, "w") as dumpFile:
		dumpFile.write("[")
		for documentId in idList:
			count += 1
			print("\r\t", count, "/", total, "\t", end="")
			if count % 100 == 0:
				sys.stdout.flush()
			document = myColl.find({"_id": documentId})
			try:
				document = document[0]
			except IndexError:
				print("Found not exist _id:", documentId)
				continue
			document.pop("_id")
			if (count > 1):
				dumpFile.write(",\n")
			dumpFile.write(json.dumps(document,
				ensure_ascii=False))
		dumpFile.write("]")
	print("Successfully exported.")

def _mongoImport(_host, _port, _db, _collection, _filename):
	print("Reading json file...")
	data = json.loads(open(_filename, "r").read())
	print("\tJson file successfully loaded on memory.")
	
	print("\nConnecting with", _host, "...")
	myclient = pymongo.MongoClient("mongodb://" + _host + ":" + _port + "/")
	mydb = myclient[_db]
	myColl = mydb[_collection]
	print("\tHost connected.")
	
	collCnt = myColl.count()
	if collCnt:
		print(" * The collection", _collection, "already have", collCnt,"documents.")
	print("IMPORTING data from", _filename, "to", _db, "/", _collection)
	count = 0
	total = len(data)
	for document in data:
		count += 1
		print("\r\t", count, "/", total, "\t", end="")
		if count % 100 == 0:
			sys.stdout.flush()
		myColl.insert_one(document)
	print("Successfully imported.")

if __name__ == "__main__":
	assert(len(sys.argv))
	parseAction(config, sys.argv)
	parseArgs(config, sys.argv[2:])
	varifyConfigure(config)
	if not config["noconfirm"]:
		confirmOperation(config)
	if config["action"] == mongoActionExportFlag:
		_mongoExport(config["host"], config["port"], config["db"],
			config["collection"], config["filename"])
	elif config["action"] == mongoActionImportFlag:
		_mongoImport(config["host"], config["port"], config["db"],
			config["collection"], config["filename"])
