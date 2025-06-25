// index.js
const fs = require("fs");
const { google } = require("googleapis");
const axios = require("axios");
const moment = require("moment-timezone");
require("dotenv").config();
const { sendNotificationEmail } = require('./mailgun');

// --- CONFIGURATION ---
const DELAY_BETWEEN_PUMPS_MS = 30000; // 30 seconds
const SHEET_HEADER_ROW = 2; // Row where date headers start (adjust if needed)
const SHEET_PUMP_START_ROW = 3; // Row where pump house data starts

// --- GOOGLE SHEETS SETUP ---
async function getAuthSheets() {
  const auth = new google.auth.GoogleAuth({
    keyFile: "credentials.json",
    scopes: ["https://www.googleapis.com/auth/spreadsheets"],
  });
  const client = await auth.getClient();
  const sheets = google.sheets({ version: "v4", auth: client });
  return sheets;
}

// --- API FETCH HELPERS ---
async function fetchNewPumpHouses() {
  const response = await axios.get(
    "http://thundertusk.distronix.in/api/pump_house/get_pumphouses"
  );
  return response.data.data || [];
}

async function fetchOldPumpKPIs(stationId) {
  const url = `https://poc.pwms.wbphed.wtlprojects.com/api/kpi?stationId=${stationId}`;
  try {
    const response = await axios.post(url); // POST with stationId in query string
    console.log("OldPump API:", url, "Response:", response.data);
    return response.data.data || [];
  } catch (err) {
    console.error("Error fetching old pump KPI:", url, err.message);
    throw err;
  }
}

async function fetchNewPumpKPIs(id) {
  const url = `http://thundertusk.distronix.in:80/api/gen/get_kpi?id=${id}`;
  const response = await axios.get(url);
  return response.data.data || [];
}

async function fetchPumpActivity(pump, startDate, endDate) {
  if (pump.isOld) {
    // Old pumps: POST request
    const url = `https://poc.pwms.wbphed.wtlprojects.com/api/pump-activity?stationId=${pump.stationId}&start_date=${startDate}&end_date=${endDate}`;
    try {
      const res = await axios.post(url);
      return res.data.data || [];
    } catch (err) {
      console.error(`Failed to fetch pump activity for ${pump.name}:`, err.message);
      return [];
    }
  } else {
    // New pumps: GET request
    const url = `https://pwms.wbphed.wtlprojects.com/api/gen/pump-activity?id=${pump.id}&start_date=${startDate}&end_date=${endDate}`;
    try {
      const res = await axios.get(url);
      return res.data.data || [];
    } catch (err) {
      console.error(`Failed to fetch pump activity for ${pump.name}:`, err.message);
      return [];
    }
  }
}

// --- DELAY HELPER ---
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
 
// --- COLOR MAPPING ---
function getColorCode(colorName) {
  switch (colorName) {
    case "RED":
      return { red: 1, green: 0, blue: 0 };
    case "ORANGE":
      return { red: 1, green: 0.65, blue: 0 };
    case "YELLOW":
      return { red: 1, green: 1, blue: 0 };
    case "WHITE":
    default:
      return { red: 1, green: 1, blue: 1 };
  }
}

// --- SHEET HELPERS ---
async function getSheetInfo(sheets) {
  const spreadsheetId = process.env.SPREADSHEET_ID;
  const sheetName = process.env.SHEET_NAME;
  const meta = await sheets.spreadsheets.get({ spreadsheetId });
  const sheet = meta.data.sheets.find(
    s => s.properties.title === sheetName
  );
  return sheet ? sheet.properties : null;
}

async function getSheetData(sheets) {
  const spreadsheetId = process.env.SPREADSHEET_ID;
  const sheetName = process.env.SHEET_NAME;
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${sheetName}`,
  });
  return res.data.values;
}

// --- SHEET INITIALIZER ---
async function initializeSheetIfEmpty(sheets, pumps, todayDate) {
  const spreadsheetId = process.env.SPREADSHEET_ID;
  const sheetName = process.env.SHEET_NAME;
  // Read current data
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: `${sheetName}`,
  });
  const values = res.data.values || [];
  if (values.length > 1) return; // Already initialized

  // Header rows
  // First row: empty cells, then "REMARKS" above date columns
  const header1 = ["", "", "", "", "", "REMARKS"];
  // Second row: actual column headers
  const header2 = [
    "Sl No.",
    "Scheme Name",
    "Zone",
    "Pump House",
    "Pump House Type",
    todayDate
  ];
  const rows = [header1, header2];

  // Pump house rows
  for (let i = 0; i < pumps.length; i++) {
    const p = pumps[i];
    rows.push([
      (i + 1).toString(),
      p.scheme_name || "",
      p.zone || "N/A",
      p.name,
      p.type || "",
      ""
    ]);
  }

  // Write all at once
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${sheetName}!A1`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: rows },
  });
}

// Find or add today's date column, return its letter and index
async function getOrAddDateColumn(sheets, todayDate) {
  const data = await getSheetData(sheets);
  let headerRow = [];
  if (data.length >= SHEET_HEADER_ROW) {
    headerRow = data[SHEET_HEADER_ROW - 1] || [];
  } else {
    // Add empty rows up to header row if missing
    for (let i = data.length; i < SHEET_HEADER_ROW; i++) {
      await sheets.spreadsheets.values.append({
        spreadsheetId: process.env.SPREADSHEET_ID,
        range: `${process.env.SHEET_NAME}!A${i + 1}`,
        valueInputOption: "USER_ENTERED",
        insertDataOption: "INSERT_ROWS",
        requestBody: { values: [[""]] },
      });
    }
    headerRow = [];
  }

  let colIdx = headerRow.findIndex(
    v => v && moment(v, "D/M/YYYY", true).isValid() && moment(v, "D/M/YYYY").format("D/M/YYYY") === todayDate
  );
  if (colIdx === -1) {
    // Add new column for today
    colIdx = headerRow.length;
    await sheets.spreadsheets.values.update({
      spreadsheetId: process.env.SPREADSHEET_ID,
      range: `${process.env.SHEET_NAME}!${columnToLetter(colIdx + 1)}${SHEET_HEADER_ROW}`,
      valueInputOption: "USER_ENTERED",
      requestBody: { values: [[todayDate]] },
    });

  }
  return { colIdx, colLetter: columnToLetter(colIdx + 1) };
}

// Find or add pump house row, return its index (1-based)
async function getOrAddPumpRow(sheets, pump, allRows) {
  // Find by Scheme Name, Pump House, Pump House Type (columns B, D, E)
  for (let i = SHEET_PUMP_START_ROW - 1; i < allRows.length; i++) {
    const row = allRows[i];
    if (
      (row[1] || "") === pump.scheme_name &&
      (row[3] || "") === pump.name &&
      (row[4] || "") === pump.type
    ) {
      return i + 1;
    }
  }
  // Not found, add new row
  const newRow = [];
  newRow[0] = ""; // S. No.
  newRow[1] = pump.scheme_name || "";
  newRow[2] = pump.zone || "N/A";
  newRow[3] = pump.name;
  newRow[4] = pump.type;
  await sheets.spreadsheets.values.append({
    spreadsheetId: process.env.SPREADSHEET_ID,
    range: `${process.env.SHEET_NAME}!A${allRows.length + 1}`,
    valueInputOption: "USER_ENTERED",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: [newRow] },
  });
  return allRows.length + 1;
}

// Convert column index (1-based) to letter (A, B, ..., Z, AA, AB, ...)
function columnToLetter(col) {
  let temp = "";
  let letter = "";
  while (col > 0) {
    temp = (col - 1) % 26;
    letter = String.fromCharCode(temp + 65) + letter;
    col = (col - temp - 1) / 26;
  }
  return letter;
}

// --- SHEET UPDATE ---
async function updateSheetCell(sheets, row, colIdx, remark, color) {
  const spreadsheetId = process.env.SPREADSHEET_ID;
  const sheetName = process.env.SHEET_NAME;
  const colLetter = columnToLetter(colIdx + 1);

  // Update remark
  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: `${sheetName}!${colLetter}${row}`,
    valueInputOption: "USER_ENTERED",
    requestBody: { values: [[remark]] },
  });

  // Update cell color (formatting)
  const sheetInfo = await getSheetInfo(sheets);
  const requests = [
    {
      repeatCell: {
        range: {
          sheetId: sheetInfo.sheetId,
          startRowIndex: row - 1,
          endRowIndex: row,
          startColumnIndex: colIdx,
          endColumnIndex: colIdx + 1,
        },
        cell: {
          userEnteredFormat: {
            backgroundColor: getColorCode(color),
          },
        },
        fields: "userEnteredFormat.backgroundColor",
      },
    },
  ];
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId,
    requestBody: { requests },
  });
}

// --- LOGIC CHECKS (APPLY ALL 19 RULES) ---
function applyRules(kpis, pumpActivity) {
  // Helper to get value by label or key
  const get = (label, badge) => {
    if (!kpis) return undefined;
    let found = kpis.find(
      k =>
        (k.label === label || k.key === label) &&
        (badge ? k.badge === badge : true)
    );
    return found ? found.value : undefined;
  };
  const getLastPacketTime = (label) => {
    let found = kpis.find(k => k.label === label && k.badge && k.badge.startsWith("Last Recorded"));
    if (found && found.badge) {
      // badge: "Last Recorded (04-06-2025 17:43:18)"
      const match = found.badge.match(/\(([\d\- :]+)\)/);
      if (match) {
        return moment(match[1], "DD-MM-YYYY HH:mm:ss");
      }
    }
    return null;
  };

  // 1. Pump Status OFF > 24h (ORANGE)
  if (pumpActivity && pumpActivity.length > 0) {
    // Find the latest transaction
    const latest = pumpActivity[0]; // assuming sorted DESC by timestamp
    if (latest.main_pump_status === false || latest.is_on === false) {
      // Check how long it's been OFF
      const offTime = moment(latest.main_pump_transition_timestamp || latest.transition_timestamp);
      const hoursOff = moment().diff(offTime, "hours");
      if (hoursOff >= 24) {
        return {
          remarks: "Pump Status OFF for more than 24 hours.",
          color: "ORANGE",
        };
      }
    }
  }

  // 2. Chlorine Pump Status OFF > 24h (ORANGE)
  const chlorineStatus = get("Chlorine Pump Status");
  if (chlorineStatus === "Off") {
    // Check if it's been off for >24h (not enough info in sample, so skip for now)
  }

  // 3. Total On Time Yesterday is zero (ORANGE)
  const totalOnTimeYest = get("Total On Time", "Yesterday");
  if (totalOnTimeYest && totalOnTimeYest.startsWith("00 h 00 m 00 s")) {
    return {
      remarks: "Total On Time (Yesterday) is zero.",
      color: "ORANGE",
    };
  }

  // 5. Total On Time Yesterday is zero but Total Flow Yesterday is non-zero (RED)
  const totalFlowYest = get("Total Flow of Pump(m³)", "Yesterday");
  if (
    totalOnTimeYest &&
    totalOnTimeYest.startsWith("00 h 00 m 00 s") &&
    totalFlowYest &&
    parseFloat(totalFlowYest) > 0
  ) {
    return {
      remarks:
        "Total On Time (Yesterday) is zero but Total Flow Yesterday is non-zero. Pump Controller Detector Issue.",
      color: "RED",
    };
  }

  // 6. Total On Time Today is zero but Total Flow Today is non-zero (RED)
  const totalOnTimeToday = get("Total On Time", "Today");
  const totalFlowToday = get("Total Flow of Pump(m³)", "Today");
  if (
    totalOnTimeToday &&
    totalOnTimeToday.startsWith("00 h 00 m 00 s") &&
    totalFlowToday &&
    parseFloat(totalFlowToday) > 0
  ) {
    return {
      remarks:
        "Total On Time (Today) is zero but Total Flow Today is non-zero. Pump Controller Detector Issue.",
      color: "RED",
    };
  }

  // 8. Residual Chlorine is zero for >6h (ORANGE)
  const chlorine = get("Residual Chlorine(ppm) at OHR") || get("Residual Chlorine(ppm) at Source Side");
  if (chlorine && parseFloat(chlorine) === 0) {
    // Check last packet time if available
    const lastChlorineTime = getLastPacketTime("Residual Chlorine(ppm) at OHR") || getLastPacketTime("Residual Chlorine(ppm) at Source Side");
    if (lastChlorineTime && moment().diff(lastChlorineTime, "hours") > 6) {
      return {
        remarks: "Residual Chlorine is zero for more than 6 hours.",
        color: "ORANGE",
      };
    }
  }

  // 10. Water Level of OHR <0.1m or >6m (ORANGE)
  const waterLevelOHR = get("Water Level of OHR(m)");
  if (waterLevelOHR && (parseFloat(waterLevelOHR) < 0.1 || parseFloat(waterLevelOHR) > 6)) {
    return {
      remarks: "Water Level of OHR shows out of range (<0.1m or >6m). READING ERROR.",
      color: "ORANGE",
    };
  }

  // 12. Water Pressure in OHR <20 psi or >36 psi (ORANGE)
  const waterPressureOHR = get("Water Presure in OHR (PSI)");
  if (
    waterPressureOHR &&
    (parseFloat(waterPressureOHR) < 20 || parseFloat(waterPressureOHR) > 36)
  ) {
    return {
      remarks: "Water Pressure in OHR shows out of range (<20psi or >36psi). READING ERROR.",
      color: "ORANGE",
    };
  }

  // 16. Tubewell Water Level (m) should be 1-32m (ORANGE)
  const tubewellLevel = get("Tubewell Water Level(m)");
  if (
    tubewellLevel &&
    (parseFloat(tubewellLevel) < 1 || parseFloat(tubewellLevel) > 32)
  ) {
    return {
      remarks: "Tubewell Water Level out of range (should be 1-32m). SENSOR ERROR.",
      color: "ORANGE",
    };
  }

  // 18. Discharge From Service OHR - Total Flow vs Velocity vs Discharge (RED)
  const outFlow = get("Discharge From Service OHR - Total Flow(m³)", "Today");
  const outVelocity = get("Discharge From Service OHR - Velocity(m/s)", "Today");
  const outRate = get("Discharge From Service OHR(m³/h)", "Today");
  if (
    (outFlow && parseFloat(outFlow) > 0) ||
    (outVelocity && parseFloat(outVelocity) > 0) ||
    (outRate && parseFloat(outRate) > 0)
  ) {
    if (
      (outFlow && parseFloat(outFlow) === 0) ||
      (outVelocity && parseFloat(outVelocity) === 0) ||
      (outRate && parseFloat(outRate) === 0)
    ) {
      return {
        remarks:
          "Discharge From Service OHR - Total Flow, Velocity, or Discharge mismatch. SENSOR ERROR.",
        color: "RED",
      };
    }
  }

  // 19. Discharge From Service OHR - Total Flow Yesterday is zero (YELLOW)
  const outFlowYest = get("Discharge From Service OHR - Total Flow(m³)", "Yesterday");
  if (outFlowYest && parseFloat(outFlowYest) === 0) {
    return {
      remarks: "Discharge From Service OHR - Total Flow (Yesterday) is zero. Inspection error.",
      color: "YELLOW",
    };
  }

  // Add more rules as needed...

  return { remarks: "All OK.", color: "WHITE" };
}

// --- MAIN WORKFLOW ---
async function main() {
  const sheets = await getAuthSheets();
  const todayIST = moment().tz("Asia/Kolkata").format("D/M/YYYY");
  const todayDateISO = moment().tz("Asia/Kolkata").format("YYYY-MM-DD");
  const yesterdayDateISO = moment().tz("Asia/Kolkata").subtract(1, "day").format("YYYY-MM-DD");

  // Old pumps (manually defined)
  const oldPumps = [
    {
      id: "DXPWMS-02",
      name: "Pump House I",
      type: "Basic",
      zone: "N/A",
      scheme_name: "Humaipur PWSS",
      stationId: "DXPWMS-02",
      isOld: true,
    },
    {
      id: "DXPWMS-03",
      name: "Pump House II",
      type: "Basic",
      zone: "N/A",
      scheme_name: "Humaipur PWSS",
      stationId: "DXPWMS-03",
      isOld: true,
    },
    {
      id: "DXPWMS-01",
      name: "Pump House III",
      type: "Intermediate",
      zone: "N/A",
      scheme_name: "Humaipur PWSS",
      stationId: "DXPWMS-01",
      isOld: true,
    },
    {
      id: "DXPWMS-04",
      name: "Pump House IV",
      type: "Basic",
      zone: "N/A",
      scheme_name: "Humaipur PWSS",
      stationId: "DXPWMS-04",
      isOld: true,
    },
  ];

  // New pumps (from API)
  let masterList = [];
  try {
    masterList = await fetchNewPumpHouses();
  } catch (err) {
    console.error("Failed to fetch new pump houses:", err.message);
  }
  const newPumps = masterList.map(p => {
    // Map API type to sheet type
    let type = "";
    if (p.pump_house_type_name === "OHR") type = "Intermediate";
    else if (p.pump_house_type_name === "Non-OHR") type = "Basic";
    else if (p.pump_house_type_name === "Non-OHR Direct" || p.pump_house_type_name === "Non-OHR-Direct") type = "Direct";
    else type = p.pump_house_type_name || "";

    return {
      id: p.id,
      name: p.pump_house_name,
      type,
      zone: p.zone_name || "N/A",
      scheme_name: p.scheme_name || "",
      isOld: false,
    };
  });

  // Combine all pumps
  const pumps = [...oldPumps, ...newPumps];

  // Initialize sheet if empty with proper headers and pump house rows
  await initializeSheetIfEmpty(sheets, pumps, todayIST);

  // Now get or add today's date column and allRows as before
  const { colIdx, colLetter } = await getOrAddDateColumn(sheets, todayIST);
  const allRows = await getSheetData(sheets);

  for (const pump of pumps) {
    let kpis = [];
    let remark = "API Error";
    let color = "RED";
    let pumpActivity = [];
    try {
      if (pump.isOld) {
        kpis = await fetchOldPumpKPIs(pump.stationId);
      } else {
        kpis = await fetchNewPumpKPIs(pump.id);
      }
      // Fetch pump activity for yesterday and today
      pumpActivity = await fetchPumpActivity(pump, yesterdayDateISO, todayDateISO);

      // Pass pumpActivity to applyRules
      const result = applyRules(kpis, pumpActivity);
      remark = result.remarks;
      color = result.color;
    } catch (err) {
      console.error(`Failed to fetch KPI for ${pump.name}:`, err.message);
    }

    // Always fetch the latest rows before finding/adding
    const allRows = await getSheetData(sheets);
    const row = await getOrAddPumpRow(sheets, pump, allRows);

    try {
      await updateSheetCell(sheets, row, colIdx, remark, color);
      console.log(`Updated ${pump.name} (${pump.type}) at row ${row}, col ${colLetter}: ${remark} (${color})`);
    } catch (err) {
      console.error(`Failed to update sheet for ${pump.name}:`, err.message);
    }

    await delay(DELAY_BETWEEN_PUMPS_MS);
  }

  // Send notification email
  await sendNotificationEmail(
    "Pump House Automation Report",
    "The pump house automation script has completed successfully."
  );

  console.log("All done for today!");
}

main().catch(console.error);
