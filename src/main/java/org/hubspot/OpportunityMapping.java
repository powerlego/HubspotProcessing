package org.hubspot;

import org.apache.commons.collections4.functors.WhileClosure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.hubspot.utils.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Anthony Prestia
 */
public class OpportunityMapping {
    /**
     * The instance of the logger
     */
    private static final Logger logger = LogManager.getLogger(OpportunityMapping.class);


    public static void main(String[] args) throws IOException {

        XSSFWorkbook opportunityWorkbook = new XSSFWorkbook();
        XSSFSheet opportunitySheet = opportunityWorkbook.createSheet("Mahaffey Opportunity List");
        File[] deals = new File("./contacts/deals/").listFiles();
        String customForm = "Mahaffey | Rental Opportunity";
        int rowCount = 0;
        Map companyMap = readCompanyIds(new File("./MahaffeyCompanyIDs/MahaffeyCompanyIds.xlsx"));

        XSSFCellStyle header = opportunityWorkbook.createCellStyle();
        header.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        header.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        header.setBorderRight(BorderStyle.THIN);
        header.setBorderLeft(BorderStyle.THIN);
        header.setBorderBottom(BorderStyle.THIN);
        header.setBorderTop(BorderStyle.THIN);

        XSSFRow rowElement = opportunitySheet.createRow(rowCount);
        XSSFCell cell = rowElement.createCell(0);
        cell.setCellStyle(header);
        cell.setCellValue("Custom Form");
        cell = rowElement.createCell(1);
        cell.setCellStyle(header);
        cell.setCellValue("Opportunity Title (Name)");
        cell = rowElement.createCell(2);
        cell.setCellStyle(header);
        cell.setCellValue("Company");
        cell = rowElement.createCell(3);
        cell.setCellStyle(header);
        cell.setCellValue("Rental Location");
        cell = rowElement.createCell(4);
        cell.setCellStyle(header);
        cell.setCellValue("Status");
        cell = rowElement.createCell(5);
        cell.setCellStyle(header);
        cell.setCellValue("Expected Close");
        cell = rowElement.createCell(6);
        cell.setCellStyle(header);
        cell.setCellValue("Expected Installation Date");
        cell = rowElement.createCell(7);
        cell.setCellStyle(header);
        cell.setCellValue("Projected Total");
        cell = rowElement.createCell(8);
        cell.setCellStyle(header);
        cell.setCellValue("Date Created");

        rowCount++;

        for (File dealFolder : deals) {
            String contactId = dealFolder.getName();
            File[] contactDeals = dealFolder.listFiles();
            JSONObject contact = new JSONObject(FileUtils.readJsonString(logger, new File("./cache/contacts/"+contactId+".json")));
            String companyID = ((JSONObject) contact.get("properties")).get("associatedcompanyid").toString();
            String companyName;
            if (companyID.equals("null") || companyID.isEmpty()) {
                if (((JSONObject) contact.get("properties")).get("company").toString().equals("null")) {
                    companyName = null;
                }
                else {
                    companyName = ((JSONObject) contact.get("properties")).get("company").toString().strip();
                    if (companyMap.containsKey(companyName.toLowerCase())) {
                        companyName = companyMap.get(companyName.toLowerCase()) + " " + companyName;
                    }
                }
            }
            else {
                JSONObject company = new JSONObject(FileUtils.readJsonString(logger, new File("./cache/companies/"+companyID+".json")));
                companyName = ((JSONObject) company.get("properties")).get("name").toString().strip();
                if (companyMap.containsKey(companyName.toLowerCase())) {
                    companyName = companyMap.get(companyName.toLowerCase()) + " " + companyName;
                }
            }

            for (File deal : contactDeals) {
                rowElement = opportunitySheet.createRow(rowCount);
                JSONObject dealObj = new JSONObject(FileUtils.readJsonString(logger, deal));
                JSONObject properties = (JSONObject) dealObj.get("properties");
                // --------------------------------------------------------------------------

                String dealName = findJSONValue(properties, "dealname");
                String siteLocation = findJSONValue(properties, "site_location");
                String dealStage = findJSONValue(properties, "dealstage");
                String status = convertDealstage(dealStage);
                String closeDate = findJSONValue(properties, "closedate");
                String deliveryDate = findJSONValue(properties, "delivery_date");
                String createDate = findJSONValue(properties, "hs_createdate");
                String projectedTotal = findJSONValue(properties, "hs_projected_amount");

                String[] tempDate;
                if (closeDate != null) {
                    tempDate = closeDate.split(" ");
                    closeDate = monthNum(tempDate[1]) + "/" + tempDate[2] + "/" + tempDate[5];
                }
                if (deliveryDate != null) {
                    tempDate = deliveryDate.split("-");
                    if (tempDate.length == 3) {
                        deliveryDate = tempDate[1] + "/" + tempDate[2] + "/" + tempDate[0];
                    }
                }
                if (createDate != null) {
                    tempDate = createDate.split(" ");
                    createDate = monthNum(tempDate[1]) + "/" + tempDate[2] + "/" + tempDate[5];
                }

                rowElement.createCell(0).setCellValue(customForm);
                rowElement.createCell(1).setCellValue(dealName);
                rowElement.createCell(2).setCellValue(companyName);
                rowElement.createCell(3).setCellValue(siteLocation);
                rowElement.createCell(4).setCellValue(status);
                rowElement.createCell(5).setCellValue(closeDate);
                rowElement.createCell(6).setCellValue(deliveryDate);
                rowElement.createCell(7).setCellValue(projectedTotal);
                rowElement.createCell(8).setCellValue(createDate);

                rowCount++;
            }

        }

        try {
            FileOutputStream outputStream = new FileOutputStream("./contacts/opportunityMapping.xlsx");
            opportunityWorkbook.write(outputStream);
        } catch (IOException e) {
            logger.fatal("An error occured while writing excel files.");
        }


    }

    public static Map<String, String> readCompanyIds(File excelFile) throws IOException {
        Map<String, String> companyInfo = new HashMap<>();
        FileInputStream file = new FileInputStream(excelFile);
        XSSFWorkbook workbook = new XSSFWorkbook(file);
        XSSFSheet sheet = workbook.getSheet("Customers");

        int i = 0;
        Iterator<Row> rowIterator = sheet.iterator();
        while (rowIterator.hasNext()) {
            if (i == 0) {
                rowIterator.next();
                i++;
            }
            Row row = rowIterator.next();

            String companyID = row.getCell(1).toString();
            String companyName = row.getCell(2).toString().strip();
            if (companyInfo.containsKey(companyName)) {
                if (companyInfo.get(companyName).matches("^CUST.*$")) {
                    companyInfo.put(companyName.toLowerCase(), companyID);
                }
            }
            else {
                companyInfo.put(companyName.toLowerCase(), companyID);
            }
            i++;
        }
        return companyInfo;
    }


    public static String convertDealstage(String code) {
        if (code.equals("closedlost")) {
            return "0% Closed lost";
        }
        else if (code.equals("qualifiedtobuy")) {
            return "10% Proposal Sent";
        }
        else if (code.equals("presentationscheduled")) {
            return "20% Proposal Progressing";
        }
        else if (code.equals("decisionmakerboughtin")) {
            return "80% Contract Sent";
        }
        else if (code.equals("6845926")) {
            return "99% Contract Progressing";
        }
        else if (code.equals("closedwon")) {
            return "100% Closed Won";
        }
        else if (code.equals("f68150ea-2e74-498c-8c8b-ec2ecdbe407b")) {
            return "0% Creating Proposal";
        }
        else {
            return null;
        }
    }

    public static String findJSONValue(JSONObject jsonObject, String key) {
        String value = null;

        if (jsonObject.has(key)) {
            if (!jsonObject.get(key).toString().equals("null")) {
                value = jsonObject.get(key).toString();
            }
        }

        return value;
    }

    public static int monthNum(String month) {
        int num = 0;
        switch (month) {
            case "Jan":
                num = 1;
                break;
            case "Feb":
                num = 2;
                break;
            case "Mar":
                num = 3;
                break;
            case "Apr":
                num = 4;
                break;
            case "May":
                num = 5;
                break;
            case "Jun":
                num = 6;
                break;
            case "Jul":
                num = 7;
                break;
            case "Aug":
                num = 8;
                break;
            case "Sep":
                num = 9;
                break;
            case "Oct":
                num = 10;
                break;
            case "Nov":
                num = 11;
                break;
            case "Dec":
                num = 12;
        }
        return num;
    }

}
