'use strict';

const BigQuery = require('@google-cloud/bigquery');
const Storage = require('@google-cloud/storage');
const outboundHandler = require('./outboundCalls.js');


// Instantiates clients
const bigquery = new BigQuery({
    projectId: process.env.PROJECT_ID
});


exports.isCorrectFolder = function (file) {
    var fileName = file.name.substr(file.name.lastIndexOf('/')).toLowerCase().trim();
    var filePath = file.name.substr(0, file.name.lastIndexOf('/')).trim();
    var targetPaths = process.env.TRIGGER_FILE_PATHS.trim().split(',');

    for (var i = 0; i < targetPaths.length; i++) {
        var targetPath = targetPaths[i].replace(`gs://${file.bucket}/`, '').trim();
        if (filePath == targetPath && fileName.includes('sales_forecast')) {
            return true;
        }
    }
    return false;
};


exports.validateCheckSum = function (file) {

    return new Promise(function (resolve, reject) {

        const storage = Storage();
        storage
            .bucket(file.bucket)
            .file(file.name)
            .getMetadata()
            .then(results => {
                    const metadata = results[0];
                    getCheckSumFromDTS(file.name).then(
                        response => {
                            resolve(metadata.md5Hash === response.responsebody);
                        })
                }
            )
            .catch(err => {
                console.error('ERROR:', err);
                reject();
            });
    });
}


function getCheckSumFromDTS(fileName) {

    var fixedFileName = fileName.replace("relex_output/", "");
    return Promise.resolve(outboundHandler.getCheckSumFromDTS(fixedFileName));

}


exports.decompressOutput = function (file) {

    var storage = new Storage({projectId: process.env.PROJECT_ID});
    var tableId = getTableID(file);

    const metadata = {
        sourceFormat: 'CSV',
        skipLeadingRows: 1,
        autodetect: false,
        writeDisposition: 'WRITE_APPEND',
        fieldDelimiter: process.env.DELIMITER,
        allowJaggedRows: 'TRUE'
    };

    console.log(`The file ${file.name} has been successfully picked up and going to be stored in table ${process.env.PROJECT_ID}:${process.env.DATASET_ID}.${tableId}.`);
    outboundHandler.sendStatusUpdate(file.name, process.env.IN_PROGRESS_CODE);
    var errorsState = false;

// Loads data from a Google Cloud Storage file into the table
    bigquery
        .dataset(process.env.DATASET_ID)
        .table(tableId)
        .load(storage.bucket(file.bucket).file(file.name), metadata)
        .then(results => {
                const job = results[0];

                // load() waits for the job to finish
                console.log(`Job ${job.id} completed for file ${file.name}.`);

                // Check the job's status for errors
                const errors = job.status.errors;

                if (errors && errors.length > 0) {
                    console.log(`Errors: ${JSON.stringify(errors)}`);
                    errorsState = true;
                    throw errors;
                }
                else if (job.status.state == 'DONE') {

                    if (!errorsState) {
                        console.log(`Data from file ${file.name} has been successfully stored in ${process.env.PROJECT_ID}:${process.env.DATASET_ID}.${tableId}.`);
                        outboundHandler.sendStatusUpdate(file.name, process.env.SUCCESS_CODE).then( response => {
                            if (response.responsebody.includes("Updated")) {
                                outboundHandler.checkRunStatus(file.name);
                            } else {
                                outboundHandler.sendStatusEmail(`The cloud function did not get an appropriate response from DataTransferService when 
                                    updating the file status. Please check to see that the file (${file.name}) exists in the DataTransferService 
                                    tracking tables and that the file name shown here is correct.`)
                            }
                        });
                    }

                }
            }
        ).catch(err => {
        console.error(`Error storing data from '${file.bucket}/${file.name}' in ${process.env.PROJECT_ID}:${process.env.DATASET_ID}.${tableId}:`, err);
        outboundHandler.sendStatusUpdate(file.name, process.env.FAILURE_CODE);
        outboundHandler.sendStatusEmail(`The file '${file.bucket}/${file.name}' has failed to store in ${process.env.PROJECT_ID}:${process.env.DATASET_ID}.${tableId}.
            Please re-play the file into the appropriate bucket, so we can re-trigger the cloud function as a work-around for this error.`);
    });

    console.log(`BQ job has started for file ${file.name}`)

};

function getTableID(file) {
    var fileName = (file.name.split('/')).pop();
    if (fileName.includes('daily')) {
        return process.env.RELEX_OUTPUT_DAILY_TABLE_NAME;
    }
    else if (fileName.includes('weekly')) {
        return process.env.RELEX_OUTPUT_WEEKLY_TABLE_NAME;
    }

    return process.env.RELEX_OUTPUT_TABLE_NAME;
}



