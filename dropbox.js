/*
 * Endpoints: https://www.dropbox.com/developers/documentation/http/documentation
 */

const axios = require('axios');
const { parse } = require('path');

const { DROPBOX_TOKEN } = process.env;

/*
 * Global variables
 */

const list = []; // list of all files

exports.getFileContent = async path => {
  const request = {
    url: 'https://content.dropboxapi.com/2/files/download',
    method: 'post',
    headers: {
      Authorization: `Bearer ${DROPBOX_TOKEN}`,
      'Content-Type': 'text/plain',
      'Dropbox-API-Arg': JSON.stringify({ path })
    }
  };

  try {
    response = await axios(request);

    return response.data;
  } catch (err) {
    console.error(err.response);
    return false;
  }
};

const listContinue = async cursor => {
  const request = {
    url: 'https://api.dropboxapi.com/2/files/list_folder/continue',
    method: 'post',
    headers: {
      Authorization: `Bearer ${DROPBOX_TOKEN}`,
      'Content-Type': 'application/json'
    },
    data: {
      cursor
    }
  };

  try {
    response = await axios(request);

    return response.data;
  } catch (err) {
    console.error(err);
    return false;
  }
};

const listFiles = async path => {
  const request = {
    url: 'https://api.dropboxapi.com/2/files/list_folder',
    method: 'post',
    headers: {
      Authorization: `Bearer ${DROPBOX_TOKEN}`,
      'Content-Type': 'application/json'
    },
    data: {
      include_deleted: false,
      include_has_explicit_shared_members: false,
      include_media_info: false,
      include_mounted_folders: true,
      include_non_downloadable_files: true,
      recursive: true,
      path
    }
  };

  try {
    response = await axios(request);
    return response.data;
  } catch (err) {
    console.error(err.response);
    return false;
  }
};

const processFiles = files => {
  const { entries } = files;

  entries?.forEach(entry => {
    const tag = entry['.tag'];
    if (tag === 'file') {
      const { name, path_display, client_modified, rev, id } = entry;
      list.push({
        name,
        path: path_display,
        modified: client_modified,
        rev,
        id
      });
    }
  });
};

exports.getList = async () => {
  let files = await listFiles('/DataVizFolder/');
  processFiles(files);
  let { has_more } = files;
  let { cursor } = files;
  while (has_more) {
    let files = await listContinue(cursor);
    processFiles(files);
    has_more = files.has_more;
    cursor = files.cursor;
  }
};

exports.getCsvList = () => {
  const csv = [];

  for (let i = 0; i < list.length; ++i) {
    if (list[i].path.endsWith('.csv')) csv.push(list[i]);
  }

  return csv;
};

exports.getProjectFiles = predicate => {
  const projectFiles = list.filter(predicate);

  const filesMap = projectFiles.reduce((acc, file) => {
    const { ext, name } = parse(file.name);

    if (!acc[name]) {
      acc[name] = {};
    }

    acc[name][ext.replace('.', '')] = file;

    return acc;
  }, {});

  return Object.values(filesMap);
};
