const fs = require('fs');
const express = require("express");
const Q = require("q");

// Each line has the format ((nasip-addr, dslam), number-of-users)
const lineRegex = /\(\((.+),(.+)\),(.+)\)/;
// Location of files
const sessionsDirectory = "/tmp/streamoutput/sessions/";
const cdrRateDirectory = "/tmp/streamoutput/cdrRate/";
const cdrRateDerivativeDirectory = "/tmp/streamoutput/cdrRateDerivative/";
// Name of this process
process.title="flowview";

// Instantiate express
var app=express();

app.use("/", express.static(__dirname));

app.get("/", function(req, res){
    res.json({});
});

app.get("/sessions", function(req, res){
    serveFile(res, sessionsDirectory)
});

app.get("/cdrRate", function(req, res){
    serveFile(res, cdrRateDirectory)
});

app.get("/cdrRateDerivative", function(req, res){
    serveFile(res, cdrRateDerivativeDirectory)
});

app.listen(7182, function () {
    console.log('FlowView listening on port 7182');
});

function serveFile(res, dirName){
    Q.nfcall(fs.readdir, dirName)
        .then(function(files){
            var response = [];
            var fileIndex = 0;
            addFileToResponse();

            function addFileToResponse(){
                if(fileIndex == files.length){
                    // All the files have been read. Send the response
                    res.json(response);
                } else {
                    fileIndex++;
                    if(files[fileIndex-1].match("^part-[0-9]{5}$")){
                        fs.readFile(dirName+files[fileIndex-1], function(err, data){
                            if(err) console.log("error "+err.message);
                            else {
                                var lines = data.toString().split(("\n"));
                                var responseLine;
                                for(var i = 0; i < lines.length; i++){
                                    var matches = lineRegex.exec(lines[i]);
                                    if(matches && matches.length == 4) response.push([matches[1], matches[2], parseFloat(matches[3])]);
                                }
                            }
                            // Continue with next file
                            addFileToResponse();
                        });
                    } else {
                        // Continue with next file
                        addFileToResponse();
                    }
                }
            }
        })
        .catch(function(error){
            res.status(500).send(error.message);
        })
}

