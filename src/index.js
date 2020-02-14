/**
 * @typedef Dirent
 * @property {() => boolean} isBlockDevice
 * @property {() => boolean} isCharacterDevice
 * @property {() => boolean} isDirectory
 * @property {() => boolean} isFIFO
 * @property {() => boolean} isFile
 * @property {() => boolean} isSocket
 * @property {() => boolean} isSymbolicLink
 * @property {string} name
 */

const startTime = performance.now();

const fs = require('fs');
const path = require('path');
const readLine = require('readline');
const { performance } = require('perf_hooks');

const inputDir = path.resolve('files');
const outputDir = path.resolve('outputs');


getFilesFrom(inputDir)
  .then(getCSV)
  .then(parseCSV);


/**
 * Get all files from a directory.
 * @param {string} dir Path of target directory
 * @returns {Promise<Dirent[]>}
 */
function getFilesFrom(dir){
  return new Promise((res, rej) => {
    fs.readdir(dir, { withFileTypes: true }, function(err, dirents){
      if (err)
        rej(err);
      else
        res(dirents);
    });
  });
}

/**
 * Filter CSVs from an array of Dirents.
 * @param {Dirent[]} dirents Array of Dirents
 * @returns {Dirent[]}
 */
function getCSV(dirents){
  const ret = [];

  for (const dirent of dirents){
    if (!dirent.isFile()){
      continue;
    }

    const ext = path.extname(dirent.name);

    if (ext.toLowerCase() !== '.csv'){
      continue;
    }

    ret.push(dirent);
  }

  return ret;
}

/**
 * Parse an array of CSV files into respective JSON files.
 * @param {Dirent[]} csvs Array of CSV Dirent
 */
async function parseCSV(csvs){
  let i = 0;

  for (const csv of csvs){
    try {
      const filePath = path.resolve(inputDir, csv.name);
      const outputPath = path.resolve(outputDir, path.basename(csv.name, '.csv') + '.json');
      const json = await toJSON(filePath);
      await writeFile(outputPath, json);
      i++;
    } catch(e){
      console.log(`\nConversion of ${csv.name} failed with the following error:`);
      console.error(e);
    }
  }

  const endTime = performance.now();
  const timeDistance = endTime - startTime;
  const roundedTime = Math.round(timeDistance * 1000) / 1000;

  console.log(`Converted ${i} file${i === 1 ? '' : 's'} in ${roundedTime}ms.`);
}

/**
 * Convert CSV content into JSON.
 * @param {string} path Full file path.
 * @returns {Promise<string>} Stringified JSON.
 */
function toJSON(path){
  return new Promise((res, rej) => {
    const stream = fs.createReadStream(path, { encoding: 'utf-8' });
    const reader = readLine.createInterface({ input: stream });

    let isFirstLine = true;
    let content = [];
    let headers;

    reader.on('line', function(line){
      if (isFirstLine){
        isFirstLine = false;
        headers = line.split(',');
      } else {
        const columns = line.split(',');
        content.push(createData(headers, columns));
      }
    });

    reader.on('error', rej);

    reader.on('close', function(){
      content = JSON.stringify(content, null, 2);
      res(content);
    });
  });
}

/**
 * Create an object with headers as keys and columns as JSON parsed values.
 * @param {string[]} headers Headers of CSV
 * @param {string[]} columns Columns of CSV
 * @return {Object.<string, *>}
 */
function createData(headers, columns){
  return headers.reduce((ret, header, i) => {
    try {
      ret[header] = JSON.parse(columns[i]);
    } catch(e){
      ret[header] = columns[i];
    } finally {
      return ret;
    }
  }, {});
}

/**
 * Write a stringified JSON into a file.
 * @param {string} outputPath Full output path.
 * @param {string} content Stringified JSON.
 * @returns {Promise<void>}
 */
function writeFile(outputPath, content){
  return new Promise((res, rej) => {
    fs.writeFile(outputPath, content, err => {
      if (err)
        rej(err);
      else
        res();
    });
  });
}
