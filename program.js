
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
    if (err) {
        console.log(err);
        process.exit(0);
    } else {
        console.log('\n\nConnection successful.\n\n');
        readFile();
    }
});

function readFile() {
    _rl.question('File to read (no validation): ', (fileName) => {

    });
}


