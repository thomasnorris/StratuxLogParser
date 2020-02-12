
var _sql = require('mssql');
var _csv = require('csv-parser');
var _fs = require('fs');
var _rl = require('readline');
var _ssh = require('node-ssh');
_ssh = new _ssh();

const SQL_CFG = {
    user: 'sa',
    password: 'admin',
    server: 'localhost\\sqlexpress',
    database: 'StratuxLogs',
    port: 1433
}

const SSH_CFG = {
    port: 22,
    host: '192.168.10.1',
    username: 'pi',
    password: 'raspberry',
    tryKeyboard: true
}

const REMOTE_LOG_DIR = '/var/log/';
const LOCAL_LOG_DIR = process.env.USERPROFILE + '\\Desktop\\';

const CSV_TABLE = 'dbo.SensorDataCSV';
const LOG_TABLE = 'dbo.StratuxDataLog';
const FILE_INFO_TABLE = 'dbo.ParsedFileInfo';
const CSV_MATCH_REGEX = /sensors.+.csv/g;
const LOG_MATCH_REGEX = /stratux.log/g;

(async function() {
    await dbConnect();
    await sshConnect();

    var matchingFiles = await getMatchingFiles();
    var file = await chooseAndLoadFile(matchingFiles);
    var results = await parseFile(file);

    exit(results.table + ' updated with ' + results.count + ' new records.\nFile info ID: ' + file.infoID);
})();

async function sshConnect() {
    return new Promise((resolve, reject) => {
        _ssh.connect(SSH_CFG)
            .then(() => {
                console.log('SSH connection successful.\n');
                resolve();
            })
            .catch((err) => {
                exit(err);
            });
    });
}

async function dbConnect() {
    return new Promise((resolve, reject) => {
        _sql.connect(SQL_CFG, (err) => {
            if (err)
                exit(err);

            console.log('\nDatabase connection successful.');
            resolve();
        });
    })
}

async function chooseAndLoadFile(matchingFiles) {
    var rl = _rl.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    return new Promise((resolve, reject) => {
        rl.question('\nEnter file number to parse [0 - ' + (matchingFiles.length - 1) + ']: ', (index) => {
            rl.close();

            if (!matchingFiles[index])
                exit('Undefined file.');

            var fileName = matchingFiles[index];
            var sql = 'INSERT INTO ' + FILE_INFO_TABLE + ' (file_name, date_parsed) OUTPUT Inserted.ID VALUES (\'' + fileName + '\', GETDATE())';
            sendRequest(sql, (res) => {
                resolve({
                    name: fileName,
                    localPath: LOCAL_LOG_DIR + fileName,
                    remotePath: REMOTE_LOG_DIR + fileName,
                    infoID: res.recordset[0].ID,
                    extension: fileName.match( /\.[0-9a-z]+$/g)[0].toLowerCase()
                });
            });
        });
    });
}

async function getMatchingFiles() {
    return new Promise((resolve, reject) => {
        _ssh.execCommand('ls ' + REMOTE_LOG_DIR)
            .then((result) => {
                var filesAndDirs = result.stdout.split('\n');
                console.log('Matching files in \'' + REMOTE_LOG_DIR + '\':');

                var filtered = [];
                var filterCount = 0;
                filesAndDirs.filter((file) => {
                    if (file.match(CSV_MATCH_REGEX) || file.match(LOG_MATCH_REGEX)) {
                        console.log(filterCount++ + ': ' + file);
                        filtered.push(file);
                    }
                });

                if (filtered.length === 0)
                    exit('No matching files found.');

                resolve(filtered);
            })
            .catch((err) => {
                exit(err);
            });
    });
}

async function parseFile(file) {
    console.log('\nProcessing \'' +  file.name + '\', please wait...');

    return new Promise((resolve, reject) => {
        _ssh.getFile(file.localPath, REMOTE_LOG_DIR + '/' + file.name)
            .then(() => {
                console.log('Reading from local file: ' + file.localPath);

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
                data.file_info_ID = file.infoID;
                sql = 'INSERT INTO ' + CSV_TABLE + ' (';
                rows = '';
                columns = '';

                var keys = Object.keys(data);
                keys.forEach((key) => {
                    rows += key
                    columns += data[key];

                    // last key doesn't need a comma
                    if (keys.indexOf(key) !== keys.length - 1) {
                        rows += ', ';
                        columns += ', ';
                    }
                });

                sql += rows + ') VALUES (' + columns + ');';

                sendRequest(sql, (res) => {
                    ++sentCount;
                    if (sentCount === readCount)
                        resolve({
                            table: CSV_TABLE,
                            count: sentCount
                        });
                });
            });
    }

    function parseLog(resolve, reject) {
        var readCount = 0;
        var sentCount = 0;
        var rl = _rl.createInterface({
            input: _fs.createReadStream(file.localPath)
        });

        rl.on('line', (line) => {
            ++readCount;
            var splitStr = line.split(' ');
            var date = splitStr[0];
            var time = splitStr[1];

            line = splitStr.slice(2, splitStr.length).join(' ');

            var sql = 'INSERT INTO ' + LOG_TABLE + ' (file_info_ID, date, time, message) VALUES (' + file.infoID + ', \'' + date + '\', \'' + time + '\', \'' + line + '\')';
            sendRequest(sql, (res) => {
                ++sentCount;
                if (readCount === sentCount)
                    resolve({
                        table: LOG_TABLE,
                        count: sentCount
                    });
            });
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

function exit(msg) {
    console.log('\n' + msg + '\nExiting.');
    process.exit(0);
}
