
var _sql = require('mssql');
var _csv = require('csv-parser');
var _fs = require('fs');
var _rl = require('readline');
_rl = _rl.createInterface({
    input: process.stdin,
    output: process.stdout
});

const SQL_CFG = {
    user: 'sa',
    password: 'admin',
    server: 'localhost\\sqlexpress',
    database: 'StratuxLogs',
    port: 1433
}

_sql.connect(SQL_CFG, (err) => {
    if (err)
        halt(err);

    console.log('\n\nConnection successful.\n\n');
    readFile();
});

function readFile() {
    _rl.question('File path to read with extension [no validation][.log, .csv, .txt]: ', (fileName) => {
        _rl.close();

        // grab extension
        var regex = /\.[0-9a-z]+$/i;
        var ext = fileName.match(regex)[0].toLowerCase();

        // insert into the file info table and continue
        var sql = 'INSERT INTO dbo.FileInfo (file_name, date_parsed) OUTPUT Inserted.ID VALUES (\'' + fileName + '\', GETDATE())';
        sendRequest(sql, (res) => {
            var fileInfoID = res.recordset[0].ID;
            if (ext === '.csv')
                parseCSV(fileName, fileInfoID);
            else
                parseLog(fileName, fileInfoID);
        });
    });
    //parseCSV('C:/Users/tnorris/Downloads/sensors_20191125_223549.csv', 1);
}

function parseCSV(name, fileInfoID) {
    var sql = '';
    var readCount = 0;
    var sentCount = 0;
    _fs.createReadStream(name)
        .pipe(_csv())
        .on('data', (data) => {
            ++readCount;
            data.file_info_ID = fileInfoID;
            var keys = Object.keys(data);
            sql = 'INSERT INTO dbo.DataDumpCSV (';
            keys.forEach((key) => {
                sql += key
                if (keys.indexOf(key) !== keys.length - 1)
                    sql += ', ';
            });

            sql += ') VALUES (';

            keys.forEach((key) => {
                sql += data[key];
                if (keys.indexOf(key) !== keys.length - 1)
                    sql += ', ';
            });

            sql += ')';

            sendRequest(sql, (res) => {
                ++sentCount;
                if (sentCount === readCount)
                    halt('dbo.DataDumpCSV updated with ' + sentCount + ' new records');
            });
        })
        .on('end', (res) => {
            console.log('Read ' + readCount + ' rows.');
        });
}

function parseLog(name, fileInfoID) {

}

function sendRequest(sql, cb) {
    var req = new _sql.Request();
    req.query(sql, (err, res) => {
        if (err)
            halt(err);
        cb(res);
    })
}

function halt(err) {
    console.log(err);
    process.exit(0);
}
