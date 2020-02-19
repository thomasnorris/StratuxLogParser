
var _sql = require('mssql');
var _csv = require('csv-parser');
var _fs = require('fs');
var _rl = require('readline');
var _ssh = require('node-ssh');
_ssh = new _ssh();

const CFG_FILE = './config/config.json';
var _config = readJson(CFG_FILE);

if (!_config.log_directories.local)
    _config.log_directories.local = process.env.USERPROFILE + '\\Desktop\\';

(async function() {
    await sshConnect();
    await dbConnect();

    var matchingFiles = await getMatchingFiles();
    var file = await chooseAndLoadFile(matchingFiles);
    var results = await parseFile(file);

    exit(results.table + ' updated with ' + results.count + ' new records.\nFile info ID: ' + file.infoID);
})();

async function sshConnect() {
    return new Promise((resolve, reject) => {
        _ssh.connect(_config.ssh.connection)
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
        _sql.connect(_config.sql.connection, (err) => {
            if (err)
                exit(err);

            console.log('\nDatabase connection successful.');
            resolve();
        });
    })
}

async function chooseAndLoadFile(matchingFiles) {
    var rl = createReadlineInterface(process.stdin, process.stdout);

    return new Promise((resolve, reject) => {
        rl.question('\nEnter file number to parse [0 - ' + (matchingFiles.length - 1) + ']: ', (index) => {
            rl.close();

            if (!matchingFiles[index])
                exit('Undefined file.');

            var fileName = matchingFiles[index];
            var sql = 'INSERT INTO ' + _config.sql.tables.file_info + ' (file_name, date_parsed) OUTPUT Inserted.ID VALUES (\'' + fileName + '\', GETDATE())';
            sendRequest(sql, (res) => {
                resolve({
                    name: fileName,
                    localPath: _config.log_directories.local + fileName,
                    remotePath: _config.log_directories.remote + fileName,
                    infoID: res.recordset[0].ID,
                    extension: fileName.match(new RegExp(_config.regex.file_ext))[0].toLowerCase()
                });
            });
        });
    });
}

async function getMatchingFiles() {
    return new Promise((resolve, reject) => {
        _ssh.execCommand('ls ' + _config.log_directories.remote)
            .then((result) => {
                var filesAndDirs = result.stdout.split('\n');
                console.log('Matching files in \"' + _config.log_directories.remote + '\":');

                var filtered = [];
                var filterCount = 0;
                filesAndDirs.filter((file) => {
                    if (file.match(new RegExp(_config.regex.log_match)) || file.match(new RegExp(_config.regex.csv_match))) {
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
    console.log('\nProcessing \"' +  file.name + '\", please wait...');

    return new Promise((resolve, reject) => {
        console.log('Copying remote file to local directory...');
        _ssh.getFile(file.localPath, file.remotePath)
            .then(() => {
                console.log('Parsing and sending to database...');
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
                sql = 'INSERT INTO ' + _config.sql.tables.csv + ' (';
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
                            table: _config.sql.tables.csv,
                            count: sentCount
                        });
                });
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

            var sql = 'INSERT INTO ' + _config.sql.tables.log + ' (file_info_ID, date, time, message) VALUES (' + file.infoID + ', \'' + date + '\', \'' + time + '\', \'' + line + '\')';
            sendRequest(sql, (res) => {
                ++sentCount;
                if (readCount === sentCount)
                    resolve({
                        table: _config.sql.tables.log,
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
    console.log('\n' + msg + '\nExiting.');
    process.exit(0);
}
