/*
 * Endpoints: https://www.dropbox.com/developers/documentation/http/documentation
 */

require('dotenv').config()
const mysql = require('./utils/mysql')

const axios = require('axios');

const { DROPBOX_TOKEN } = process.env;

const dropboxTest = async () => {
    const request = {
        url: 'https://api.dropboxapi.com/2/files/list_folder',
        method: 'post',
        headers: {
            "Authorization": `Bearer ${DROPBOX_TOKEN}`,
            "Content-Type": "application/json"
        },
        data: {
            include_deleted: false,
            include_has_explicit_shared_members: false,
            include_media_info: false,
            include_mounted_folders: true,
            include_non_downloadable_files: true,
            recursive: false,
            path: "/DataVizFolder/connectedEconomy"
        }
    }

    try {
        response = await axios(request);
        console.log(response.data);
    } catch(err) {
        console.error(err);
    }
}

