var zkClient = require('node-zookeeper-client');
var async = require('async');
const util = require('util');



/**
 * List of Solr servers
 with the relative load weight for each server
 */

const solrServerWeighList = {
    [process.env.SOLR1]: Number(process.env.LOAD1),
    [process.env.SOLR2]: Number(process.env.LOAD2),
    [process.env.SOLR3]: Number(process.env.LOAD3),
}

function replaceAfterPort(url, pattern, replacement) {
    var colonDigits = /:\d+/.exec(url)[0];
    var pathIndex = url.indexOf(colonDigits) + colonDigits.length;
    var hostPort = url.substring(0, pathIndex);
    var path = url.substring(pathIndex);
    return hostPort + path.replace(pattern, replacement);
}


function zkConnectAndExecute(dto, callback) {
    if (!dto) {
        return callback(new Error('Missing DTO'));
    }

    var client = zkClient.createClient(dto.zkConnectionString, dto.zkOptions);
    client.once('connected', function () {
        callback(null, client);
        client.close();
    });
    client.connect();
}


function zkGetChildren(dto, path, callback) {
    zkConnectAndExecute(dto, function (err, client) {
        if (err) {
            return callback(err);
        }
        client.getChildren(path, callback);
    })
}


function zkGetData(dto, path, callback) {
    zkConnectAndExecute(dto, function (err, client) {
        if (err) {
            return callback(err);
        }
        client.getData(path, callback);
    });
}

function getZkInfo(options, callback) {
    var dto = { zkConnectionString: options.zkConnectionString, zkOptions: options.zk };
    async.parallel({
        liveNodes: function (callback) {
            zkGetChildren(dto, options.zkLiveNodes, function (err, children /*, stats*/) {
                if (err) {
                    return callback(err);
                }
                if (children.length == 0) {
                    return callback('Found no live Solr nodes under path \'' + options.zkLiveNodes + '\' by connecting at \'' + options.zkConnectionString + '\'');
                }
                async.map(children, function (item, callback) {
                    var url = replaceAfterPort(item, /_/g, '/');
                    callback(null, url);
                }, callback);
            });
        },
        aliases: function (callback) {
            if (options.zkAliases) {
                zkGetData(dto, options.zkAliases, function (err, data /*, stats*/) {
                    if (err) {
                        return callback(err);
                    }
                    var dataObj = data ? JSON.parse(data.toString()) : {};
                    if (dataObj.collection) {
                        var aliases = swap(dataObj.collection);
                    }
                    callback(null, aliases);
                });
            } else {
                callback();
            }
        }
    }, callback);
}


// Define options
options = {
    zkConnectionString: process.env.zkConnectionString,
    zkLiveNodes: '/live_nodes', // this is the default value of znLiveNodes
    zkAliases: '/aliases.json', // this is the default value of znAliases
    solrProtocol: 'http',
    solrCollectionsGetEndPoint: '/admin/collections?action=LIST', // Supports XML and JSON writer types
    ssh: {},
    // Passed verbatim to node-zookeeper-client
    zk: {
        sessionTimeout: 3000,
        spinDelay: 1000,
        retries: 1
    },
    // Passed verbatim to node-rest-client
    rest: {
        requestConfig: {
            timeout: 3000
        },
        responseConfig: {
            timeout: 3000
        },
        mimetypes: {
            json: ["application/json", "application/json;charset=utf-8", "application/json; charset=utf-8", "application/json;charset=UTF-8", "application/json; charset=UTF-8"],
            xml: ["application/xml", "application/xml;charset=utf-8", "application/xml; charset=utf-8", "application/xml;charset=UTF-8", "application/xml; charset=UTF-8"]
        }
    }
};



/**
 * Example 0,1,2 in 25%, 25% and 50% distribution
 * @returns {Number} - number
 */
async function generateNumber() {
    const solrNodeInfo = util.promisify(getZkInfo);
    return await solrNodeInfo(options).then((data) => {
        const solrServerList = data.liveNodes;
        let lowerBound = 0;
        let sum = 0;
        for (let i = 0; i < solrServerList.length; i++) {
            sum += solrServerWeighList[solrServerList[i]];
        }
        const solrZkeeperScoreList = solrServerList.map((item, index) => {
            const upperBound = lowerBound + (solrServerWeighList[item] / sum);
            const partion = [lowerBound, upperBound];
            lowerBound = upperBound;
            return partion;
        });
        const randomNumber = Math.random();
        const seclectedServerIndex = solrZkeeperScoreList.findIndex((item) => {
            return randomNumber >= item[0] && randomNumber < item[1];
        });
        return solrServerList[seclectedServerIndex];

    }).catch((err) => {
        throw new Error(err);
    });
}

module.exports = generateNumber;