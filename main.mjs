import { exec } from "child_process";
import axios from "axios";
import fs from "fs";
import converter from 'json-2-csv';
import mapping from "./mapping.json" with { type: "json" };
import "dotenv/config";


const api = axios.create({
    baseURL: process.env.DHIS2_URL,
    auth: {
        username: process.env.DHIS2_USERNAME,
        password: process.env.DHIS2_PASSWORD,
    },
});

function runAzCopy(command) {
    return new Promise((resolve, reject) => {
        exec(`azcopy ${command}`, (error, stdout, stderr) => {
            if (error) {
                reject(`Error: ${error.message}`);
                return;
            }
            if (stderr) {
                reject(`Stderr: ${stderr}`);
                return;
            }
            resolve(stdout);
        });
    });
}

// Example usage
async function copyBlobExample(pe) {
    try {
		const validDataElements = Object.keys(mapping)
		let allDataValues = []
		const {data:{organisationUnits}} = await api.get(`organisationUnits.json?fields=id&level=3&pageSize=3&order=name:asc`);
		for(const {id} of organisationUnits) {
			const param = new URLSearchParams({ period:pe,orgUnit:id,children:true,dataSet:"VDhwrW9DiC1" });
			const {data:{dataValues = []}} = await api.get(`dataValueSets.json?${param.toString()}`);
			allDataValues = allDataValues.concat(dataValues.filter(({dataElement}) => validDataElements.includes(dataElement)));
		}
		const csv = converter.json2csv(allDataValues,{fields:Object.keys(allDataValues[0])});
		fs.writeFileSync('Uganda_OSA_2024Q1.csv', csv);
		const result = await runAzCopy(
		    `copy "Uganda_OSA_2024Q1.csv" "${process.env.AZURE_STORAGE_ACCOUNT_SAS_URL}"`	
		);
		console.log("AzCopy operation completed:", result);
    } catch (error) {
        console.error("AzCopy operation failed:", error);
    }
}

copyBlobExample("202401");
