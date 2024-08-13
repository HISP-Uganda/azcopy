import { exec } from "child_process";
import axios from "axios";
import fs from "fs";
import converter from "json-2-csv";
import { program } from "commander";
import mapping from "./mapping.json" with { type: "json" };
import mapping_osa from "./mapping_osa.json" with { type: "json" };
import "dotenv/config";
import _ from "lodash";

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
    const {
        data: {
            listGrid: { headers, rows },
        },
    } = await api.get("sqlViews/qS5ka4GEiN7/data", {
        params: { paging: false },
    });

    const facilityCodeIndex = headers.findIndex(({ name }) => name === "uid");
    const facilityLevelIndex = headers.findIndex(
        ({ name }) => name === "hflevel"
    );
    const facilityNameIndex = headers.findIndex(({ name }) => name === "name");
    const facilityOwnerShipTypeIndex = headers.findIndex(
        ({ name }) => name === "ownership"
    );
    const geographyIdentifier1Index = headers.findIndex(
        ({ name }) => name === "region"
    );
    const geographyIdentifier2Index = headers.findIndex(
        ({ name }) => name === "district"
    );
    const facilityTypeIndex = headers.findIndex(({ name }) => name === "uid");
    const facilityOperationalStatusIndex = headers.findIndex(
        ({ name }) => name === "status"
    );

    return rows.map((row) => {
        return {
            FacilityCode: row[facilityCodeIndex],
            FacilityLevel: row[facilityLevelIndex],
            FacilityName: row[facilityNameIndex],
            FacilityOwnerShipType: row[facilityOwnerShipTypeIndex],
            GeographyIdentifier1: row[geographyIdentifier1Index],
            GeographyIdentifier2: row[geographyIdentifier2Index],
            FacilityType: "Health Facility",
            FacilityOperationalStatus: row[facilityOperationalStatusIndex],
        };
    });
}

async function copyBlobExample(pe = "LAST_MONTH") {
    try {
        // const validDataElements = Object.keys(mapping);
        const currentDate = new Date();

        const stockOnHandDataElements = Object.entries(mapping_osa)
            .flatMap(([de, val]) => {
                if (val.dataPoint === "Stock on hand") {
                    return de;
                }
                return [];
            })
            .join(";");
        const quantityUsedDataElements = Object.entries(mapping_osa)
            .flatMap(([de, val]) => {
                if (val.dataPoint === "Quantity used") {
                    return de;
                }
                return [];
            })
            .join(";");

        const stockOnHandParams = new URLSearchParams();
        stockOnHandParams.append("dimension", `dx:${stockOnHandDataElements}`);
        stockOnHandParams.append("dimension", `ou:LEVEL-5`);
        stockOnHandParams.append("dimension", `pe:${pe}`);

        const quantityUsedParams = new URLSearchParams();
        quantityUsedParams.append(
            "dimension",
            `dx:${quantityUsedDataElements}`
        );
        quantityUsedParams.append("dimension", `ou:LEVEL-5`);
        quantityUsedParams.append("dimension", "pe:LAST_3_MONTHS");
        const { data: quantityUsed } = await api.get(
            `analytics.json?${quantityUsedParams.toString()}`
        );
        const { data: stockOnHand } = await api.get(
            `analytics.json?${stockOnHandParams.toString()}`
        );
        const quantityUsedAgain = Object.entries(
            _.groupBy(quantityUsed.rows, (x) => `${x[0]}${x[1]}`)
        ).flatMap(([ke, values]) =>
            _.orderBy(values, (x) => x[2], "desc").flatMap((row, index) => {
                return {
                    ReportingUnit: "UGA",
                    FacilityCode: row[1],
                    ProductCode: mapping_osa[row[0]].code,
                    DataPoint: index === 2 ? 13 : index === 1 ? 12 : 11,
                    CurrentReportingPeriod: row[2],
                    Value: Number(row[3]),
                };
            })
        );

        const allStock = stockOnHand.rows.flatMap((row) => {
            return [
                {
                    ReportingUnit: "UGA",
                    FacilityCode: row[1],
                    ProductCode: mapping_osa[row[0]].code,
                    DataPoint: 1061,
                    CurrentReportingPeriod: row[2],
                    Value: Number(row[3]),
                },
                {
                    ReportingUnit: "UGA",
                    FacilityCode: row[1],
                    ProductCode: mapping_osa[row[0]].code,
                    DataPoint: 1064,
                    CurrentReportingPeriod: row[2],
                    Value: "DHIS2",
                },
                {
                    ReportingUnit: "UGA",
                    FacilityCode: row[1],
                    ProductCode: mapping_osa[row[0]].code,
                    DataPoint: 1066,
                    CurrentReportingPeriod: row[2],
                    Value: Number(
                        `${currentDate.getFullYear()}${String(currentDate.getMonth() + 1).padStart(2, "0")}${String(currentDate.getDate()).padStart(2, "0")}`
                    ),
                },
            ];
        });

        const csv = converter.json2csv(allStock.concat(quantityUsedAgain), {
            fields: [
                "ReportingUnit",
                "FacilityCode",
                "ProductCode",
                "DataPoint",
                "CurrentReportingPeriod",
                "Value",
            ],
        });
        fs.writeFileSync(
            `Uganda_OSA_${allStock[0].CurrentReportingPeriod}.csv`,
            csv
        );
        const result = await runAzCopy(
            `copy "Uganda_OSA_${allStock[0].CurrentReportingPeriod}.csv" "${process.env.AZURE_STORAGE_ACCOUNT_SAS_URL}"`
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
                fields: [
                    "FacilityCode",
                    "FacilityLevel",
                    "FacilityName",
                    "FacilityOwnerShipType",
                    "GeographyIdentifier1",
                    "GeographyIdentifier2",
                    "FacilityType",
                    "FacilityOperationalStatus",
                ],
            });
            fs.writeFileSync(`Uganda_Facility.csv`, csv);
            const result = await runAzCopy(
                `copy "Uganda_Facility.csv" "${process.env.AZURE_STORAGE_ACCOUNT_SAS_URL}"`
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
        await copyBlobExample();
    });

program.parse();
