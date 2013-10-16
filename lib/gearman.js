var netlib = require("net"),
    Stream = require("stream").Stream,
    utillib = require("util");

module.exports = Gearman;

if (typeof setImmediate == 'undefined') {
    var setImmediate = process.nextTick;
}

//function Gearman (host, port, debug){
function Gearman (jobServers, debug, verbose){

    Stream.call(this);

    this.debug = !!debug;
    this.verbose = !!verbose;
            
    if (jobServers) this.jobServers = jobServers
    else jobServers = [{host: "localhost", port: "4730"}];
    //this.port = port || 4730;
    //this.host = host || "localhost";

    this.init();
}
utillib.inherits(Gearman, Stream);

Gearman.prototype.init = function(){
    
    this.processingCommands   = new Array (this.jobServers.length);
    this.commandQueues        = new Array (this.jobServers.length);
    this.handleCallbackQueues = new Array (this.jobServers.length);
    this.sockets              = new Array (this.jobServers.length);
    this.serversConnected     = new Array (this.jobServers.length);
    
    for (var i = 0; i < this.jobServers.length; i++) {
        //TODO: All this should be an 'object'
        this.serversConnected[i]     = false;
        this.sockets[i]              = false;
        this.commandQueues[i]        = [];
        this.handleCallbackQueues[i] = [];
        this.processingCommands[i]   = false;
    }

    this.remainder = false;

    this.currentJobs = {};
    this.currentWorkers = {};

    this.workers = {};
}

Gearman.packetTypes = {
    CAN_DO: 1,
    CANT_DO: 2,
    RESET_ABILITIES: 3,
    PRE_SLEEP: 4,
    NOOP: 6,
    SUBMIT_JOB: 7,
    JOB_CREATED: 8,
    GRAB_JOB: 9,
    NO_JOB: 10,
    JOB_ASSIGN: 11,
    WORK_STATUS: 12,
    WORK_COMPLETE: 13,
    WORK_FAIL: 14,
    GET_STATUS: 15,
    ECHO_REQ: 16,
    ECHO_RES: 17,
    SUBMIT_JOB_BG: 18,
    ERROR: 19,
    STATUS_RES: 20,
    SUBMIT_JOB_HIGH: 21,
    SET_CLIENT_ID: 22,
    CAN_DO_TIMEOUT: 23,
    ALL_YOURS: 24,
    WORK_EXCEPTION: 25,
    OPTION_REQ: 26,
    OPTION_RES: 27,
    WORK_DATA: 28,
    WORK_WARNING: 29,
    GRAB_JOB_UNIQ: 30,
    JOB_ASSIGN_UNIQ: 31,
    SUBMIT_JOB_HIGH_BG: 32,
    SUBMIT_JOB_LOW: 33,
    SUBMIT_JOB_LOW_BG: 34,
    SUBMIT_JOB_SCHED: 35,
    SUBMIT_JOB_EPOCH: 36
};

Gearman.packetTypesReversed = {
    "1": "CAN_DO",
    "2": "CANT_DO",
    "3": "RESET_ABILITIES",
    "4": "PRE_SLEEP",
    "6": "NOOP",
    "7": "SUBMIT_JOB",
    "8": "JOB_CREATED",
    "9": "GRAB_JOB",
    "10": "NO_JOB",
    "11": "JOB_ASSIGN",
    "12": "WORK_STATUS",
    "13": "WORK_COMPLETE",
    "14": "WORK_FAIL",
    "15": "GET_STATUS",
    "16": "ECHO_REQ",
    "17": "ECHO_RES",
    "18": "SUBMIT_JOB_BG",
    "19": "ERROR",
    "20": "STATUS_RES",
    "21": "SUBMIT_JOB_HIGH",
    "22": "SET_CLIENT_ID",
    "23": "CAN_DO_TIMEOUT",
    "24": "ALL_YOURS",
    "25": "WORK_EXCEPTION",
    "26": "OPTION_REQ",
    "27": "OPTION_RES",
    "28": "WORK_DATA",
    "29": "WORK_WARNING",
    "30": "GRAB_JOB_UNIQ",
    "31": "JOB_ASSIGN_UNIQ",
    "32": "SUBMIT_JOB_HIGH_BG",
    "33": "SUBMIT_JOB_LOW",
    "34": "SUBMIT_JOB_LOW_BG",
    "35": "SUBMIT_JOB_SCHED",
    "36": "SUBMIT_JOB_EPOCH"
};

Gearman.paramCount = {
    ERROR:          ["string", "string"],
    JOB_ASSIGN:     ["string", "string", "buffer"],
    JOB_ASSIGN_UNIQ:["string", "string", "string", "buffer"],
    JOB_CREATED:    ["string"],
    WORK_COMPLETE:  ["string", "buffer"],
    WORK_EXCEPTION: ["string", "string"],
    WORK_WARNING:   ["string", "string"],
    WORK_DATA:      ["string", "buffer"],
    WORK_FAIL:      ["string"],
    WORK_STATUS:    ["string", "number", "number"]
};

Gearman.prototype.connect = function(){
            
    
    for (var i = 0; i < this.jobServers.length; i++) {
        (function(_this, i) {

            if (_this.sockets[i]) return;
         
            if(_this.debug || _this.verbose) {
                console.log("[node-gearman-ms]: Connecting to server "+i+" -> "+_this.jobServers[i].host+":"+_this.jobServers[i].port);
            }
            
            _this.sockets[i] = (netlib.connect || netlib.createConnection)(_this.jobServers[i].port||4730, _this.jobServers[i].host||"localhost");
            _this.sockets[i].on("connect", (function(){
                        
                _this.serversConnected[i] = true;
                _this.sockets[i].setKeepAlive(true);
        
                if(_this.debug || _this.verbose){
                    console.log("[node-gearman-ms]: Connected to server "+i+"-> "+_this.jobServers[i].host+":"+_this.jobServers[i].port);
                }
                _this.emit("connect");
                
                for (var func in this.workers) {
                    this.sendWorkersRegistration (i);
                }
                
                _this.processCommandQueue(i);

            }).bind(_this));
            
            _this.sockets[i].on("end",   _this.closedSocket.bind(_this, i));
            _this.sockets[i].on("close", _this.closedSocket.bind(_this, i));
            _this.sockets[i].on("error", _this.errorHandler.bind(_this, i));
            _this.sockets[i].on("data",  _this.receive.bind(_this, i));
        })(this, i);
    }
};

Gearman.prototype.closedSocket = function (serverNum) {
    
    if (this.debug || this.verbose) {
        console.log ("[node-gearman-ms]: Socket closed: "+this.jobServers[serverNum].host + ":" + this.jobServers[serverNum].port);
    }
    
    //if (this.sockets[serverNum]) this.sockets[serverNum].end();
    this.serversConnected[serverNum] = true;
    this.sockets[serverNum] = false;
    this.reconnect (serverNum);
    
};

Gearman.prototype.close = function(){

    for (var i = 0; i < this.jobServers.length; i++) {
        if(this.sockets[i]){
            try{
                this.sockets[i].on("close", null);
                this.sockets[i].end();
            }catch(E){}
        }
    }
    // clear current jobs
    for(var i in this.currentJobs){
        if(this.currentJobs.hasOwnProperty(i)){
            if(this.currentJobs[i]){
                this.currentJobs[i].abort();
                this.currentJobs[i].emit("error", new Error("Job failed"));
            }
            delete this.currentJobs[i];
        }
    }

    // clear current workers
    for(var i in this.currentWorkers){
        if(this.currentWorkers.hasOwnProperty(i)){
            if(this.currentWorkers[i]){
                this.currentWorkers[i].finished = true;
            }
            delete this.currentWorkers[i];
        }
    }
    
    this.init();
    
    this.emit ("close");
};


Gearman.prototype.errorHandler = function (serverNum, err){

    if (this.debug || this.verbose) {
        console.log ("[node-gearman-ms]: Socket from server "+this.jobServers[serverNum].host + ":" +this.jobServers[serverNum].port+" error: "+err);
    }
    
    this.emit("error", err);
    //this.closeConnection();
    this.sockets[serverNum].end();
    this.sockets[serverNum] = false;
    //this.reconnect (serverNum);
};

Gearman.prototype.reconnect = function (serverNum) {
    var _this = this;
    if (this.debug || this.verbose) {
        console.log ("[node-gearman-ms]: Reconnect: in 5 seconds to server "+this.jobServers[serverNum].host + ":" +this.jobServers[serverNum].port);
    }
    
    setTimeout((function() {
        _this.connect ();
    }), 5000);
 
}


Gearman.prototype.processCommandQueue = function(serverNum, chunk){
    if (this.debug) {
        console.log ("processCommandQueue: serverNum:"+serverNum);
        console.log ("processCommandQueue: chunk:"+chunk);
    }
    var command;
    if(this.commandQueues[serverNum].length){
        this.processingCommands[serverNum] = true;
        command = this.commandQueues[serverNum].shift();
        this.sendCommandToServer.apply (this, [serverNum, command]);
    }else{
        this.processingCommands[serverNum] = false;
    }
};

Gearman.prototype.sendCommand = function(){
    
    var args   = Array.prototype.slice.call(arguments);
    var serverNum = args[0];
    var command = args.slice(1);
    if (this.debug) {
        console.log ("sendCommand: serverNum: "+serverNum);
        console.log ("sendCommand: command: "+command);
    }
    this.commandQueues[serverNum].push(command);
    if(!this.processingCommands[serverNum]){
        this.processCommandQueue(serverNum);
    }
};

Gearman.prototype.sendCommandToServer = function(){
    var body,
        serverNum = arguments[0],
        args = Array.prototype.slice.call(arguments[1]),
        commandName, commandId, commandCallback,
        i, len, bodyLength = 0, curpos = 12;
                
    if(!this.serversConnected[serverNum]){
        this.commandQueues[serverNum].unshift(args);
        return this.connect();
    }

    commandName = (args.shift() || "").trim().toUpperCase();

    if(args.length && typeof args[args.length-1] == "function"){
        commandCallback = args.pop();
        this.handleCallbackQueue[serverNum].push(commandCallback);
    }

    commandId = Gearman.packetTypes[commandName] || 0;

    if(!commandId){
        // TODO: INVALID COMMAND!
    }

    for(i=0, len=args.length; i<len; i++){
        if(!(args[i] instanceof Buffer)){
            args[i] = new Buffer((args[i] || "").toString(), "utf-8");
        }
        bodyLength += args[i].length;
    }

    bodyLength += args.length>1 ? args.length - 1 : 0;

    body = new Buffer(bodyLength + 12); // packet size + 12 byte header

    body.writeUInt32BE(0x00524551, 0); // \0REQ
    body.writeUInt32BE(commandId, 4); // \0REQ
    body.writeUInt32BE(bodyLength, 8); // \0REQ

    // compose body
    for(i=0, len = args.length; i<len; i++){
        args[i].copy(body, curpos);
        curpos += args[i].length;
        if(i < args.length-1){
            body[curpos++] = 0x00;
        }
    }

    if(this.debug){
        console.log("Sending: " + commandName + " with "+args.length+" params");
        console.log(" - ", body);
        args.forEach(function(arg, i){
            console.log("  - ARG:"+i+" ", arg.toString());
        });
    }

    if (this.debug) {
        console.log ("sendCommandToServer serverNum: "+serverNum);
    }
    this.sockets[serverNum].write(body, this.processCommandQueue.bind(this, serverNum));
};

Gearman.prototype.receive = function(serverNum, chunk){
    
    if (this.debug) {
        console.log ("receive: serverNum: %j", serverNum);
        console.log ("receive: chunk: %j",chunk);
    }
    
    var data = new Buffer((chunk && chunk.length || 0) + (this.remainder && this.remainder.length || 0)),
        commandId, commandName,
        bodyLength = 0, args = [], argTypes, curarg, i, len, argpos, curpos;

    // nothing to do here
    if(!data.length){
        return;
    }
    
    // if theres a remainder value, tie it together with the incoming chunk
    if(this.remainder){
        this.remainder.copy(data, 0, 0);
        if(chunk){
            chunk.copy(data, this.remainder.length, 0);
        }
    }else{
        data = chunk;
    }
    
    // response header needs to be at least 12 bytes
    // otherwise keep the current chunk as remainder
    if(data.length<12){
        this.remainder = data;
        return;
    }

    if(data.readUInt32BE(0) != 0x00524553){
        // OUT OF SYNC!
        return this.errorHandler(new Error("Out of sync with server"));
    }

    // response needs to be 12 bytes + payload length
    bodyLength = data.readUInt32BE(8);
    if(data.length < 12 + bodyLength){
        this.remainder = data;
        return;
    }

    // keep the remainder if incoming data is larger than needed
    if(data.length > 12 + bodyLength){
        this.remainder = data.slice(12 + bodyLength);
        data = data.slice(0, 12 + bodyLength);
    }else{
        this.remainder = false;
    }

    commandId = data.readUInt32BE(4);
    commandName = (Gearman.packetTypesReversed[commandId] || "");
    if(!commandName){
        // TODO: UNKNOWN COMMAND!
        return;
    }

    if(bodyLength && (argTypes = Gearman.paramCount[commandName])){
        curpos = 12;
        argpos = 12;

        for(i = 0, len = argTypes.length; i < len; i++){

            if(i < len - 1){
                while(data[curpos] !== 0x00 && curpos < data.length){
                    curpos++;
                }
                curarg = data.slice(argpos, curpos);
            }else{
                curarg = data.slice(argpos);
            }

            switch(argTypes[i]){
                case "string":
                    curarg = curarg.toString("utf-8");
                    break;
                case "number":
                    curarg = Number(curarg.toString()) || 0;
                    break;
            }

            args.push(curarg);

            curpos++;
            argpos = curpos;
            if(curpos >= data.length){
                break;
            }
        }
    }

    if(this.debug){
        console.log("Received: " + commandName + " with " + args.length + " params");
        console.log(" - ", data);
        args.forEach(function(arg, i){
            console.log("  - ARG:"+i+" ", arg.toString());
        });
    }

    // Run command
    if(typeof this["receive_" + commandName] == "function"){
        if(commandName == "JOB_CREATED" && this.handleCallbackQueues[serverNum].length){
            args = args.concat(this.handleCallbackQueues[serverNum].shift());
        }
        this["receive_" + commandName].apply(this, [serverNum, args]);
    }

    // rerun receive just in case there's enough data for another command
    if(this.remainder && this.remainder.length>=12){
        setImmediate(this.receive.bind(this, serverNum));
    }
};

Gearman.prototype.receive_NO_JOB = function(){
    this.sendCommand(arguments[0], "PRE_SLEEP");
    this.emit("idle");
};

Gearman.prototype.receive_NOOP = function() {
    if (this.debug) {
        console.log ("receive_NOOP: workers: ", this.currentWorkers);
    }
    if (Object.keys(this.currentWorkers).length  <= 0) {
        this.sendCommand(arguments[0], "GRAB_JOB");
    }
};

Gearman.prototype.receive_ECHO_REQ = function(payload){
    this.sendCommand(arguments[0], "ECHO_RES", payload);
};

Gearman.prototype.receive_ERROR = function(code, message){
    if(this.debug){
        console.log("Server error: ", code, message);
    }
};

Gearman.prototype.receive_JOB_CREATED = function(handle, callback){
    if (this.debug) {
        console.log ("receive_JOB_CREATED: handle: %j callback: %j", handle, callback);
    }
    if(typeof callback == "function"){
        callback(handle);
    }
};

Gearman.prototype.receive_WORK_FAIL = function(handle){
    var job;
    if((job = this.currentJobs[handle])){
        delete this.currentJobs[handle];
        if(!job.aborted){
            job.abort();
            job.emit("error", new Error("Job failed"));
        }
    }
};

Gearman.prototype.receive_WORK_DATA = function(handle, payload){
    if(this.currentJobs[handle] && !this.currentJobs[handle].aborted){
        this.currentJobs[handle].emit("data", payload);
        this.currentJobs[handle].updateTimeout();
    }
};

Gearman.prototype.receive_WORK_COMPLETE = function(handle, payload){
    var job;
    if((job = this.currentJobs[handle])){
        delete this.currentJobs[handle];
        if(!job.aborted){
            clearTimeout(job.timeoutTimer);
            
            if(payload){
                job.emit("data", payload);
            }
            
            job.emit("end");    
        }
    }
};

Gearman.prototype.receive_JOB_ASSIGN = function(serverNum, params){
    
    var handle    = params[0];
    var name      = params[1];
    var payload   = params[2];
        
    if(typeof this.workers[name] == "function"){
        
        if (this.debug) {
            console.log ("receive_JOB_ASSIGN handle: %j name: %j payload: %j", handle, name, payload);
        }
        
        var worker = new this.Worker(this, serverNum, handle, name, payload);
        this.currentWorkers[handle] = worker;
        this.workers[name](payload, worker);
    }
};

Gearman.prototype.registerWorker = function(name, func){
    
//     for (var i = 0; i < this.jobServers.length; i++) {
//         if(!this.workers[name]){
//             this.sendCommand(i, "CAN_DO", name);
//             this.sendCommand(i, "PRE_SLEEP");
//         }
//     }
    this.workers[name] = func;
};

Gearman.prototype.sendWorkersRegistration = function (numServer) {
    for (var name in this.workers) {
        this.sendCommand(numServer, "CAN_DO", name);
    }
    this.sendCommand(numServer, "PRE_SLEEP");
}

Gearman.prototype.submitJob = function(name, payload, uniq, options){
    return new this.Job(this, name, payload, uniq, (typeof options != "object" ? {} : options));
};

// WORKER

Gearman.prototype.Worker = function(gearman, serverNum, handle, name, payload){
    Stream.call(this);

    this.gearman = gearman;
    this.serverNum = serverNum;
    this.handle = handle;
    this.name = name;
    this.payload = payload;
    this.finished = false;
    this.writable = true;
};
utillib.inherits(Gearman.prototype.Worker, Stream);



Gearman.prototype.Worker.prototype.write = function(data){
    if(this.finished){
        return;
    }
    this.gearman.sendCommand(this.serverNum, "WORK_DATA", this.handle, data);
};

Gearman.prototype.Worker.prototype.end = function(data){
        
    if(this.finished){
        return;
    }
    this.finished = true;
    this.gearman.sendCommand(this.serverNum, "WORK_COMPLETE", this.handle, data);
    delete this.gearman.currentWorkers[this.handle];
    
    for (var i = 0; i < this.gearman.jobServers.length; i++) {
        this.gearman.sendCommand(i, "PRE_SLEEP");
    }
};

Gearman.prototype.Worker.prototype.error = function(error){
    if(this.finished){
        return;
    }
    this.finished = true;
    this.gearman.sendCommand(this.serverNum, "WORK_FAIL", this.handle);
    delete this.gearman.currentWorkers[this.handle];
        for (var i = 0; i < this.gearman.jobServers.length; i++) {
        this.gearman.sendCommand(i, "PRE_SLEEP");
    }
};

// JOB
Gearman.prototype.Job = function(gearman, name, payload, uniq, options){
    Stream.call(this);

    this.gearman = gearman;
    this.name = name;
    this.payload = payload;

    this.timeoutTimer = null;

    var jobType = "SUBMIT_JOB";
    if (typeof options == "object") {
        if (typeof options.priority == "string" &&
            ['high', 'low'].indexOf(options.priority) != -1) {
            jobType += "_" + options.priority.toUpperCase();
        }
    
        if (typeof options.background == "boolean" && options.background)
            jobType += "_BG";
    }

    
    for (var i = 0; i < this.jobServers.length; i++) {
        gearman.sendCommand(i, jobType, name, uniq ? uniq : false, payload, !options.background ? this.receiveHandle.bind(this) : false);
    }
};
utillib.inherits(Gearman.prototype.Job, Stream);

Gearman.prototype.Job.prototype.setTimeout = function(timeout, timeoutCallback){
    this.timeoutValue = timeout;
    this.timeoutCallback = timeoutCallback;
    this.updateTimeout();
}

Gearman.prototype.Job.prototype.updateTimeout = function(){
    if(this.timeoutValue){
        clearTimeout(this.timeoutTimer);
        this.timeoutTimer = setTimeout(this.onTimeout.bind(this), this.timeoutValue);
    }
}

Gearman.prototype.Job.prototype.onTimeout = function(){
    if(this.handle){
        delete this.gearman.currentJobs[this.handle];
    }
    if(!this.aborted){
        this.abort();
        var error = new Error("Timeout exceeded for the job");
        if(typeof this.timeoutCallback == "function"){
            this.timeoutCallback(error);
        }else{
            this.emit("timeout", error);
        }
    }
}

Gearman.prototype.Job.prototype.abort = function(){
    clearTimeout(this.timeoutTimer);
    this.aborted = true;
}

Gearman.prototype.Job.prototype.receiveHandle = function(handle){
    if(handle){
        this.handle = handle;
        this.gearman.currentJobs[handle] = this;
    }else{
        this.emit("error", new Error("Invalid response from server"));
    }
};
