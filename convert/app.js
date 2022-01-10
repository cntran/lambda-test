const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const ConvertAPI = require('convertapi');
const path = require('path');
const axios = require('axios');

const STORAGE_BUCKET = process.env.STORAGE_BUCKET;
const SOURCE_FOLDER = 'original';
const DESTINATION_FOLDER = 'processed';
const FILE_EXTENSIONS = ['.doc', '.docx', '.ppt', '.pptx', '.xls', '.xlsx'];
const IMAGE_EXTENSIONS = ['.jpg', '.png', '.tiff', '.gif'];

exports.convert = async (event) => {
  const file = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));

  if (fileInValidFolder(file)) {
    const convertApiSecret = await getSecret('ConvertapiSecret');
    const converter = new ConvertAPI(JSON.parse(convertApiSecret).CONVERTAPI);
    const signedUrl = getSignedUrl(event.Records[0].s3.bucket.name, file, 1800);

    switch (getFileType(file)) {
      case 'document':
        await processFile(file, signedUrl, converter, 'pdf');
        break;

      case 'image':
        await processImage(file, signedUrl, converter, 'compress');
        break;
    }
  }
};

const fileInValidFolder = (file) => {
  const dirName = path.dirname(file).split('/');
  if (dirName.includes('original')) {
    return true;
  }
  console.log('Files in this folder will not be processed: ', dirName);

  return false;
};

const getSignedUrl = (bucket, file, expiration) => {
  return s3.getSignedUrl('getObject', {
    Bucket: bucket,
    Key: file,
    Expires: expiration
  });
};

const getFileType = (file) => {
  if (isDocument(file)) {
    return 'document';
  }
  if (isImage(file)) {
    return 'image';
  }
};

const isDocument = (file) => {
  const extension = path.extname(file);
  if (!FILE_EXTENSIONS.includes(extension)) {
    return false;
  }
  return true;
};

const isImage = (file) => {
  const extension = path.extname(file);
  if (!IMAGE_EXTENSIONS.includes(extension)) {
    return false;
  }
  return true;
};

const isJpeg = (file) => {
  const extension = path.extname(file);
  return extension === '.jpg';
};

const getJpegUrl = async (url, converter) => {
  const resultPromise = await converter.convert('jpg', { File: url });
  console.log('Converted image to jpeg', resultPromise.response);
  return resultPromise.response.Files[0].Url;
};

const processFile = async (file, url, converter, type) => {
  const conversion = await converter.convert(type, { File: url });
  const savePath = `${path.dirname(file).replace(SOURCE_FOLDER, DESTINATION_FOLDER)}/${conversion.response.Files[0].FileName}`;
  const convertedFile = await axios.get(conversion.response.Files[0].Url, { responseType: 'stream' });

  return s3
    .upload({
      Bucket: STORAGE_BUCKET,
      Key: savePath,
      Body: convertedFile.data
    })
    .promise();
};

const processImage = async (file, url, converter, type) => {
  if (isJpeg(file)) {
    await processFile(file, url, converter, type);
  } else {
    await processFile(file, await getJpegUrl(url, converter), converter, type);
  }
};

const getSecret = async function (secretName) {
  const region = process.env.AWS_REGION;
  const client = new AWS.SecretsManager({
    region: region
  });

  return new Promise((resolve, reject) => {
    client.getSecretValue({ SecretId: secretName }, function (err, data) {
      if (err) {
        reject(err);
      } else {
        if ('SecretString' in data) {
          resolve(data.SecretString);
        } else {
          const buff = new Buffer.alloc(data.SecretBinary, 'base64');
          resolve(buff.toString('ascii'));
        }
      }
    });
  });
};
