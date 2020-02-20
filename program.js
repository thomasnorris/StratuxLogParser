
var _sql = require('mssql');
var _csv = require('csv-parser');
var _fs = require('fs');
var _rl = require('readline');
var _ssh = require('node-ssh');
_ssh = new _ssh();

const CFG_FILE = './config/config.json';
var _config = readJson(CFG_FILE);

if (!_config.directories.log.local)
    _config.directories.log.local = process.env.USERPROFILE + '\\Desktop\\';

(async function() {
    // ssh, copy and prepare file for processing
    await sshConnect();
    var matchingFiles = await getMatchingFiles();
    var file = await chooseMatchingFile(matchingFiles);
    //await copyRemoteFileToLocal(file);

    // process copied local file after establishing an internet connection
    //await promptDisconnectFromStratux();
    await dbConnect();
    var results = await processMatchingFile(file);

    notify('\"' + file.name + '\" updated \"' + results.table + '\" updated with ' + results.count + ' new records.', true);
    notify('file_info_ID: ' + file.info_ID);
    notify('Process complete!', true);
    exit();
})();

async function sshConnect() {
    return new Promise((resolve, reject) => {
        _ssh.connect(_config.ssh.connection)
            .then(() => {
                notify('SSH connection to \"' + _config.ssh.connection.host + '\" successful.');
                resolve();
            })
            .catch(err => {
                exit(err);
            });
    });
}

async function dbConnect() {
    return new Promise((resolve, reject) => {
        _sql.connect(_config.sql.connection, (err) => {
            if (err)
                exit(err);

            notify('Database connection to \"' + _config.sql.connection.server + '\" successful.', true);
            resolve();
        });
    })
}

async function chooseMatchingFile(matchingFiles) {
    notify('Matching files in \"' + _config.directories.log.remote + '\": ', true);
    for (var i = 0; i < matchingFiles.length; ++i)
        notify(i + ': ' + matchingFiles[i]);

    return new Promise((resolve, reject) => {
        var rl = createReadlineInterface(process.stdin, process.stdout);
        rl.question('\nEnter file number to parse [0 - ' + (matchingFiles.length - 1) + ']: ', (index) => {
            rl.close();

            var fileName = matchingFiles[index];
            if (!matchingFiles[index])
                exit('Undefined file.');

            resolve({
                name: fileName,
                localPath: _config.directories.log.local + fileName,
                remotePath: _config.directories.log.remote + fileName,
                extension: fileName.match(new RegExp(_config.regex.file_ext))[0].toLowerCase()
            });
        });
    });
}

async function getMatchingFiles() {
    return new Promise((resolve, reject) => {
        _ssh.execCommand('ls ' + _config.directories.log.remote)
            .then((result) => {
                var filesAndDirs = result.stdout.split('\n');
                var filtered = [];
                filesAndDirs.filter((file) => {
                    if (file.match(new RegExp(_config.regex.log_match)) || file.match(new RegExp(_config.regex.csv_match)))
                        filtered.push(file);
                });

                if (filtered.length === 0)
                    exit('No matching files found.');

                resolve(filtered);
            })
            .catch(err => {
                exit(err);
            });
    });
}

async function promptDisconnectFromStratux() {
    return new Promise(resolve => {
        var rl = createReadlineInterface(process.stdin, process.stdout);
        rl.question('Disconnect from Stratux and reconnect to the internet before continuing [enter]: ', () => {
            resolve();
        });
    });
}

async function copyRemoteFileToLocal(file) {
    return new Promise(resolve => {
        notify('Copying remote file to \"' + _config.directories.log.local + '\", please wait...');
        _ssh.getFile(file.localPath, file.remotePath)
            .then(() => {
                resolve();
            })
            .catch(err => {
                exit(err);
            });
    });
}

async function processMatchingFile(file) {
    return new Promise((resolve, reject) => {
        var sql = 'INSERT INTO ' + _config.sql.tables.file_info + ' (file_name, date_parsed) OUTPUT Inserted.ID VALUES (\'' + file.name + '\', GETDATE())';
        sendRequest(sql, (res) => {
            file.info_ID = res.recordset[0].ID;
            notify('Parsing and sending to database, please wait...');
            if (file.extension === '.csv')
                parseCSV(resolve, reject);
            else
                parseLog(resolve, reject);
        });
    });

    function parseCSV(resolve, reject) {
        var sql;
        var rows;
        var columns;
        var readCount = 0;
        var sentCount = 0;

        _fs.createReadStream(file.localPath)
            .pipe(_csv())
            .on('data', (data) => {
                ++readCount;
                data.file_info_ID = file.info_ID;
                sql = 'INSERT INTO ' + _config.sql.tables.csv + ' (';
                rows = '';
                columns = '';

                var keys = Object.keys(data);
                keys.forEach((key) => {
                    rows += key;
                    columns += data[key];

                    // last key doesn't need a comma
                    if (keys.indexOf(key) !== keys.length - 1) {
                        rows += ', ';
                        columns += ', ';
                    }
                });

                sql += rows + ') VALUES (' + columns + ');';
                ((i) => {
                    setTimeout(() => {
                        sendRequest(sql, (res) => {
                            ++sentCount;
                            if (readCount === sentCount)
                                resolve({
                                    table: _config.sql.tables.csv,
                                    count: sentCount
                                });
                        });
                    }, i * _config.sql.request_delay_ms);
                })(readCount);
            });
    }

    function parseLog(resolve, reject) {
        var readCount = 0;
        var sentCount = 0;
        var rl = createReadlineInterface(_fs.createReadStream(file.localPath));

        rl.on('line', (line) => {
            ++readCount;
            var splitStr = line.split(' ');
            var date = splitStr[0];
            var time = splitStr[1];

            line = splitStr.slice(2, splitStr.length).join(' ');

            var sql = 'INSERT INTO ' + _config.sql.tables.log + ' (file_info_ID, date, time, message) VALUES (' + file.info_ID + ', \'' + date + '\', \'' + time + '\', \'' + line + '\')';
            ((i) => {
                setTimeout(() => {
                    sendRequest(sql, (res) => {
                        ++sentCount;
                        if (readCount === sentCount)
                            resolve({
                                table: _config.sql.tables.log,
                                count: sentCount
                            });
                    });
                }, i * _config.sql.request_delay_ms);
            })(readCount);
        });
    }
}

function sendRequest(sql, cb) {
    var req = new _sql.Request();
    req.query(sql, (err, res) => {
        if (err)
            exit(err);
        cb(res);
    });
}

function readJson(filePath) {
    return JSON.parse(_fs.readFileSync(filePath, 'utf8'));
}

function createReadlineInterface(input, output) {
    var ops = {};
    if (input)
        ops.input = input;

    if (output)
        ops.output = output;

    return _rl.createInterface(ops);
}

function exit(msg) {
    if (msg)
        notify(msg, true);

    notify('Exiting.');
    process.exit(0);
}

function notify(msg, newline) {
    if (newline)
        msg = '\n' + msg;
    console.log(msg);
}
