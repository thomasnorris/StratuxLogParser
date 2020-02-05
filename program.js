
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

_sql.connect(SQL_CFG, (err) => {
    if (err)
        halt(err);

    console.log('\n\nConnection successful.\n');
    afterSqlConnect();
});

async function afterSqlConnect() {
    var rl = _rl.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    var files = await listMatchingFiles();

    rl.question('\nEnter file number to parse [0 - ' + (files.length - 1) + ']: ', (index) => {
        rl.close();

        if (!files[index])
            halt('Undefined file.');

        var fileName = files[index];
        var sql = 'INSERT INTO ' + FILE_INFO_TABLE + ' (file_name, date_parsed) OUTPUT Inserted.ID VALUES (\'' + fileName + '\', GETDATE())';
        sendRequest(sql, (res) => {
            console.log('\nParsing \'' +  filePath + '\', please wait...');
            var filePath = LOG_DIR + fileName;
            var fileInfoID = res.recordset[0].ID;
            if (fileName.match( /\.[0-9a-z]+$/g)[0].toLowerCase() === '.csv')
                parseCSV(filePath, fileInfoID);
            else
                parseLog(filePath, fileInfoID);
        });
    });
}

async function listMatchingFiles() {
    return new Promise((res, rej) => {
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
            res(filtered);
        });
    });
}

function parseCSV(filePath, fileInfoID) {
    var sql;
    var rows;
    var columns;
    var readCount = 0;
    var sentCount = 0;
    _fs.createReadStream(filePath)
        .pipe(_csv())
        .on('data', (data) => {
            ++readCount;
            data.file_info_ID = fileInfoID;
            var keys = Object.keys(data);

            // build and send each query
            sql = 'INSERT INTO ' + CSV_TABLE + ' (';
            rows = '';
            columns = '';
            keys.forEach((key) => {
                rows += key
                columns += data[key];

                // don't add a comma on the last read key
                if (keys.indexOf(key) !== keys.length - 1) {
                    rows += ', ';
                    columns += ', ';
                }
            });

            sql += rows + ') VALUES (' + columns + ');';

            sendRequest(sql, (res) => {
                ++sentCount;
                // reading will finish before sending, only complete when both are done
                if (sentCount === readCount)
                    halt(CSV_TABLE + ' updated with ' + sentCount + ' new records.');
            });
        })
}

function parseLog(filePath, fileInfoID) {
    var readCount = 0;
    var sentCount = 0;
    var rl = _rl.createInterface({
        input: _fs.createReadStream(filePath)
    });

    rl.on('line', (line) => {
        ++readCount;
        var splitStr = line.split(' ');
        var date = splitStr[0];
        var time = splitStr[1];

        line = splitStr.slice(2, splitStr.length).join(' ');

        var sql = 'INSERT INTO ' + LOG_TABLE + ' (file_info_ID, date, time, message) VALUES (' + fileInfoID + ', \'' + date + '\', \'' + time + '\', \'' + line + '\')';
        sendRequest(sql, (res) => {
            ++sentCount;
            // reading will finish before sending, only complete when both are done
            if (readCount === sentCount)
                halt(LOG_TABLE + ' updated with ' + sentCount + ' new records.')
        });
    });
}

function sendRequest(sql, cb) {
    var req = new _sql.Request();
    req.query(sql, (err, res) => {
        if (err)
            halt(err);
        cb(res);
    })
}

function halt(msg) {
    console.log('\n' + msg + '\nExiting.');
    process.exit(0);
}
