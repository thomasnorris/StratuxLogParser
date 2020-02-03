
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

const CSV_TABLE = 'dbo.SensorCSVData';
const LOG_TABLE = 'dbo.StratuxLogData';
const FILE_INFO_TABLE = 'dbo.ParsedFileInfo';

_sql.connect(SQL_CFG, (err) => {
    if (err)
        halt(err);

    console.log('\n\nConnection successful.\n');
    readFile();
});

function readFile() {
    var rl = _rl.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    var fileName = 'stratux.log'

    rl.question('File path to read with extension [no validation][.log, .txt, .csv]: ', (fileName) => {
        rl.close();

        // grab extension
        var regex = /\.[0-9a-z]+$/i;
        var ext = fileName.match(regex)[0].toLowerCase();

        // insert into FILE_INFO_TABLE table and send the ID alone
        var sql = 'INSERT INTO ' + FILE_INFO_TABLE + ' (file_name, date_parsed) OUTPUT Inserted.ID VALUES (\'' + fileName + '\', GETDATE())';
        sendRequest(sql, (res) => {
            console.log('Parsing \'' + fileName + '\', please wait...');
            var fileInfoID = res.recordset[0].ID;
            if (ext === '.csv')
                parseCSV(fileName, fileInfoID);
            else
                parseLog(fileName, fileInfoID);
        });
    });
}

function parseCSV(name, fileInfoID) {
    var sql;
    var rows;
    var columns;
    var readCount = 0;
    var sentCount = 0;
    _fs.createReadStream(name)
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

function parseLog(name, fileInfoID) {
    var readCount = 0;
    var sentCount = 0;
    var rl = _rl.createInterface({
        input: _fs.createReadStream(name)
    });

    rl.on('line', (line) => {
        ++readCount;
        var splitStr = line.split(' ');
        var date = splitStr[0];
        var time = splitStr[1];

        line = splitStr.slice(2, splitStr.length).join(' ');

        var sql = 'INSERT INTO ' + LOG_TABLE + ' (file_info_ID, date, time, data_string) VALUES (' + fileInfoID + ', \'' + date + '\', \'' + time + '\', \'' + line + '\')';
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
    console.log(msg + '\nExiting.');
    process.exit(0);
}
