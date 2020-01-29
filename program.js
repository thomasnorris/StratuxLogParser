
var _sql = require('mssql');
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
        var query = 'INSERT INTO dbo.FileInfo (file_name, date_parsed) OUTPUT Inserted.ID VALUES (\'' + fileName + '\', GETDATE())';
        sendRequest(query, (res) => {
            var fileInfoID = res.recordset[0].ID;
            if (ext === '.csv')
                parseCSV(name, fileInfoID);
            else
                parseLog(name, fileInfoID);
        });
    });
}

function parseCSV(name, fileInfoID) {

}

function parseLog(name, fileInfoID) {

}

function sendRequest(query, cb) {
    var req = new _sql.Request();
    req.query(query, (err, res) => {
        if (err)
            halt(err);
        cb(res);
    })
}

function halt(err) {
    console.log(err);
    process.exit(0);
}
