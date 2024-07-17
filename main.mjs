import { exec } from "child_process";
import axios from "axios";
import fs from "fs";
import converter from "json-2-csv";
import { program } from "commander";
import "dotenv/config";

const mapping = import("./mapping.json", { with: { type: "json" } });

const api = axios.create({
    baseURL: process.env.DHIS2_URL,
    auth: {
        username: process.env.DHIS2_USERNAME ?? "",
        password: process.env.DHIS2_PASSWORD ?? "",
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

async function fetchFacilities() {
    let pageCount = 1;
    let page = 0;
    let allFacilities = [];
    while (page < pageCount) {
        console.log(`Working on page ${page + 1} of ${pageCount}`);
        const {
            data: {
                organisationUnits,
                pager: { pageCount: currentCount },
            },
        } = await api.get(`organisationUnits.json`, {
            params: {
                page: ++page,
                level: 5,
                pageSize: 1000,
                fields: "id,name,code,parent[parent[id,name,code]],organisationUnitGroups[id,name]",
            },
        });

        allFacilities = allFacilities.concat(
            organisationUnits.map(
                ({
                    id: facility_id,
                    name: facility_name,
                    parent,
                    organisationUnitGroups,
                }) => {
                    const facility_type =
                        organisationUnitGroups.find(
                            ({ id }) => id === "ou6is72lmDC"
                        )?.name ?? "";
                    return {
                        facility_name,
                        facility_code: facility_id,
                        district: parent.parent.id,
                        facility_id,
                        district_name: parent.parent.name,
                        district_code: parent.parent.id,
                        facility_type,
                    };
                }
            )
        );
        pageCount = currentCount;
    }
    return allFacilities;
}

async function copyBlobExample(pe) {
    let allDataValues = [];
    try {
        const validDataElements = Object.keys(mapping);

        const {
            data: { organisationUnits },
        } = await api.get(
            `organisationUnits.json?fields=id,name&level=3&pageSize=3&order=name:asc`
        );
        let total = 0;
        for (const { id, name } of organisationUnits) {
            console.log(
                `Querying data for ${name} (${++total} of ${organisationUnits.length})`
            );
            const param = new URLSearchParams();
            param.append("period", pe);
            param.append("orgUnit", id);
            param.append("children", "true");
            param.append("dataSet", "VDhwrW9DiC1");

            const {
                data: { dataValues = [] },
            } = await api.get(`dataValueSets.json?${param.toString()}`);

            const filtered = dataValues.filter(({ dataElement }) =>
                validDataElements.includes(dataElement)
            );
            allDataValues = allDataValues.concat(
                filtered.map(({ dataElement, period, orgUnit, value }) => ({
                    description: "",
                    reportingUnit: "UGA",
                    facilityCode: orgUnit,
                    productCode: dataElement,
                    productDescription: "",
                    reportingPeriod: period,
                    value,
                }))
            );
            break;
        }
        const csv = converter.json2csv(allDataValues, {
            fields: Object.keys(allDataValues[0]),
        });
        fs.writeFileSync(`Uganda_OSA_${pe}.csv`, csv);
        const result = await runAzCopy(
            `copy "Uganda_OSA_2024Q1.csv" "${process.env.AZURE_STORAGE_ACCOUNT_SAS_URL}"`
        );
        console.log("AzCopy operation completed:", result);
    } catch (error) {
        console.error("AzCopy operation failed:", error);
    }
}

program
    .command("sync")
    .description("Sync facilities")
    .action(async () => {
        try {
            const facilities = await fetchFacilities();
            const csv = converter.json2csv(facilities, {
                fields: Object.keys(facilities[0]),
            });
            fs.writeFileSync(`Uganda_OSA_Facilities.csv`, csv);
            const result = await runAzCopy(
                `copy "Uganda_OSA_Facilities.csv" "${process.env.AZURE_STORAGE_ACCOUNT_SAS_URL}"`
            );
            console.log("AzCopy operation completed:", result);
        } catch (error) {
            console.error("AzCopy operation failed:", error);
        }
    });

program
    .command("azcopy")
    .description("Copy using AzCopy ")
    .action(async () => {
        await copyBlobExample("202401");
    });

program.parse();
