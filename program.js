
var _sql = require('mssql');
var _csv = require('csv-parser');
var _fs = require('fs');
var _rl = require('readline');

const SQL_CFG = {
    user: 'sa',
    password: 'admin',
    server: 'localhost\\sqlexpress',
    database: 'StratuxLogs',
    port: 1433
}

const CSV_TABLE = 'dbo.SensorDataCSV';
const LOG_TABLE = 'dbo.StratuxDataLog';
const FILE_INFO_TABLE = 'dbo.ParsedFileInfo';
const CSV_MATCH_REGEX = /sensors.+.csv/g;
const LOG_MATCH_REGEX = /stratux.log/g;
// const LOG_DIR = '/var/log/'
const LOG_DIR = 'C:/Users/tnorris/Desktop/';


(async function() {
    await dbConnect();

    var file = await loadFile();
    var results = await parseFile(file);

    exit(results.table + ' updated with ' + results.count + ' new records.\nFile info ID: ' + file.infoID);
})();

async function dbConnect() {
    return new Promise((resolve, reject) => {
        _sql.connect(SQL_CFG, (err) => {
            if (err)
                exit(err);

            console.log('\n\nConnection successful.\n');
            resolve();
        });
    })
}

async function loadFile() {
    var rl = _rl.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    var files = await listMatchingFiles();

    return new Promise((resolve, reject) => {
        rl.question('\nEnter file number to parse [0 - ' + (files.length - 1) + ']: ', (index) => {
            rl.close();

            if (!files[index])
                exit('Undefined file.');

            var fileName = files[index];
            var sql = 'INSERT INTO ' + FILE_INFO_TABLE + ' (file_name, date_parsed) OUTPUT Inserted.ID VALUES (\'' + fileName + '\', GETDATE())';
            sendRequest(sql, (res) => {
                resolve({
                    name: fileName,
                    fullPath: LOG_DIR + fileName,
                    infoID: res.recordset[0].ID,
                    extension: fileName.match( /\.[0-9a-z]+$/g)[0].toLowerCase()
                });
            });
        });
    });

    async function listMatchingFiles() {
        return new Promise((resolve, reject) => {
            _fs.readdir(LOG_DIR, (err, files) => {
                console.log('Matching files in \'' + LOG_DIR + '\':');

                var filtered = [];
                var filterCount = 0;
                files.filter((file) => {
                    if (file.match(CSV_MATCH_REGEX) || file.match(LOG_MATCH_REGEX)) {
                        console.log(filterCount++ + ': ' + file);
                        filtered.push(file);
                    }
                });

                resolve(filtered);
            });
        });
    }
}

async function parseFile(file) {
    console.log('\nParsing \'' +  file.name + '\', please wait...');
    return new Promise((resolve, reject) => {
        if (file.extension === '.csv')
            parseCSV(resolve, reject);
        else
            parseLog(resolve, reject);
    });

    function parseCSV(resolve, reject) {
        var sql;
        var rows;
        var columns;
        var readCount = 0;
        var sentCount = 0;

        _fs.createReadStream(file.fullPath)
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
            input: _fs.createReadStream(file.fullPath)
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
