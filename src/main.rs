#[macro_use]
extern crate diesel;

mod db;
mod models;
mod schema;

use crate::models::NewYourModel;
use crate::db::{establish_connection, bulk_insert_your_models, bulk_update_your_models, bulk_select_your_models, select_specific_column};
use polars::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::fs;
use std::io::{BufReader, BufRead};
use std::error::Error;
use std::path::Path;
use std::time::Instant;
use std::io::Write;

fn main()-> Result<(), Box<dyn Error>> {
    let start = Instant::now();
    let start1 = Instant::now();
    // Establish database connection
    let mut connection = establish_connection();
    let end1 = Instant::now();

    let duration1 = end1.duration_since(start1);

    println!("Connection established: {:?}", duration1);

    // Read input file for bulk select
    let start2 = Instant::now();
    let select_file_path = "select.csv";
    let file_contents = fs::read_to_string(select_file_path)?;
    let first_line = file_contents.lines().next().ok_or("Empty file")?;
    let column_names: Vec<String> = first_line.split(',').map(|s| s.trim().to_string()).collect();
    let select_values = bulk_select_from_csv(select_file_path, &column_names)
        .unwrap_or_else(|err| {
            eprintln!("Error selecting values from CSV: {:?}", err);
            vec![] // Return an empty vector if an error occurs
        });
    bulk_select_your_models(&mut connection, &column_names[0], select_values);
    let end2 = Instant::now();

    let duration2 = end2.duration_since(start2);
    println!("Bulk select completed: {:?}", duration2);


    let start3 = Instant::now();
    let select_file_path = "select.csv";
    let file_contents = fs::read_to_string(select_file_path)?;
    let first_line = file_contents.lines().next().ok_or("Empty file")?;
    let column_names: Vec<String> = first_line.split(',').map(|s| s.trim().to_string()).collect();
    let select_values = bulk_select_from_csv(select_file_path, &column_names)
        .unwrap_or_else(|err| {
            eprintln!("Error selecting values from CSV: {:?}", err);
            vec![] // Return an empty vector if an error occurs
        });
    let ids = select_specific_column(&mut connection, "id",&column_names[0], select_values);
    let end3 = Instant::now();
    let duration3 = end3.duration_since(start3);
    println!("Select completed: {:?}", duration3);


    // Filtering and creating new file
    let start4 = Instant::now();
    // Load IDs into HashMap for efficient lookup
    let mut id_map: HashMap<String, bool> = HashMap::new();
    for id in ids {
        id_map.insert(id, true);
    }
    let input_file_path = "input.csv";

    // Read input file in a more efficient way
    let mut data_map: HashMap<String, String> = HashMap::new();
    let file = File::open(Path::new(input_file_path))?;
    let mut reader = BufReader::new(file);

    // Assuming header row exists (adjust logic if needed)
    let mut header_line = String::new();
    reader.read_line(&mut header_line)?;

    for line in reader.lines() {
        let line = line?;
        // Extract key (ID) and value (remaining data)
        let mut fields = line.split(',');
        let id = fields.next().unwrap_or("").to_string();
        let remaining_data = fields.collect::<Vec<_>>().join(",");

        data_map.insert(id, remaining_data);
    }
    let filtered_data: Vec<String> = data_map.into_iter()
        .filter(|(id, _data)| !id_map.contains_key(id))
        .map(|(id, data)| format!("{},{}", id, data))
        .collect();
    let new_file_path = "filtered_data.csv";
    let mut new_file = fs::File::create(new_file_path)?;
    let header = "id,uid,adjdate,adjtype,remitter,beneficiery,response,txndate,txntime,rrn,terminalid,ben_mobile_no,rem_mobile_no,chbdate,chbref,txnamount,adjamount,rem_payee_psp_fee,ben_fee,ben_fee_sw,adjfee,npcifee,remfeetax,benfeetax,npcitax,adjref,bankadjref,adjproof,compensation_amount,adjustment_raised_time,no_of_days_for_penalty,shdt73,shdt74,shdt75,shdt76,shdt77,transaction_type,transaction_indicator,beneficiary_account_number,remitter_account_number,aadhar_number,mobile_number,payer_psp,payee_psp,upi_transaction_id,virtual_address,dispute_flag,reason_code,mcc,originating_channel\n";
    write!(new_file, "{}", header)?;
    for line in filtered_data {
        write!(new_file, "{}\n", line)?;
    }
    let end4 = Instant::now();
    let duration4 = end4.duration_since(start4);
    println!("Created new file for bulk insert: {:?}", duration4);


    // Read input file for bulk insert
    let start5 = Instant::now();
    let file_path = "filtered_data.csv";
    let new_models = read_csv_to_new_models(file_path)?;
    let batch_size = 1310;
    bulk_insert_your_models(&mut connection, new_models, batch_size);
    let end5 = Instant::now();

    let duration5 = end5.duration_since(start5);

    println!("Bulk insert completed: {:?}", duration5);
    
    // Read input file for bulk update
    let start6 = Instant::now();
    let update_file_path = "update.csv";
    let updated_models = read_csv_to_updated_models(update_file_path)?;
    bulk_update_your_models(&mut connection, updated_models);
    let end6 = Instant::now();
    let duration6 = end6.duration_since(start6);

    println!("Bulk update completed: {:?}", duration6);

    let end = Instant::now();

    let duration = end.duration_since(start);

    println!("Total time elapsed: {:?}", duration);

    Ok(())
}


fn read_csv_to_new_models(file_path: &str) -> Result<Vec<NewYourModel>, Box<dyn std::error::Error>> {
    // Read CSV file and convert to NewYourModel
    let reader = BufReader::new(File::open(file_path).expect("Failed to open file"));
    let df = CsvReader::new(reader)
        .infer_schema(Some(50))
        .has_header(true)
        .finish()
        .unwrap();

    let mut new_models = Vec::new();
    for row in 0..df.height() {
        let id = df.column("id").unwrap().str().unwrap().get(row).unwrap_or_else(|| "Id not found").to_string();
        let uid = df.column("uid").unwrap().str().unwrap().get(row).unwrap_or_else(|| "Uid not found").to_string();
        let adjdate = df.column("adjdate").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjdate not found").to_string();
        let adjtype = df.column("adjtype").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjtype not found").to_string();
        let remitter = df.column("remitter").unwrap().str().unwrap().get(row).unwrap_or_else(|| "remitter not found").to_string();
        let beneficiery = df.column("beneficiery").unwrap().str().unwrap().get(row).unwrap_or_else(|| "beneficiery not found").to_string();
        let response = df.column("response").unwrap().str().unwrap().get(row).unwrap_or_else(|| "response not found").to_string();
        let txndate = df.column("txndate").unwrap().str().unwrap().get(row).unwrap_or_else(|| "txndate not found").to_string();
        let txntime = df.column("txntime").unwrap().str().unwrap().get(row).unwrap_or_else(|| "txntime not found").to_string();
        let rrn = df.column("rrn").unwrap().str().unwrap().get(row).unwrap_or_else(|| "rrn not found").to_string();
        let terminalid = df.column("terminalid").unwrap().str().unwrap().get(row).unwrap_or_else(|| "terminalid not found").to_string();
        let ben_mobile_no = df.column("ben_mobile_no").unwrap().str().unwrap().get(row).unwrap_or_else(|| "ben_mobile_no not found").to_string();
        let rem_mobile_no = df.column("rem_mobile_no").unwrap().str().unwrap().get(row).unwrap_or_else(|| "rem_mobile_no not found").to_string();
        let chbdate = df.column("chbdate").unwrap().str().unwrap().get(row).unwrap_or_else(|| "chbdate not found").to_string();
        let chbref = df.column("chbref").unwrap().str().unwrap().get(row).unwrap_or_else(|| "chbref not found").to_string();
        let txnamount = df.column("txnamount").unwrap().str().unwrap().get(row).unwrap_or_else(|| "txnamount not found").to_string();
        let adjamount = df.column("adjamount").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjamount not found").to_string();
        let rem_payee_psp_fee = df.column("rem_payee_psp_fee").unwrap().str().unwrap().get(row).unwrap_or_else(|| "rem_payee_psp_fee not found").to_string();
        let ben_fee = df.column("ben_fee").unwrap().str().unwrap().get(row).unwrap_or_else(|| "ben_fee not found").to_string();
        let ben_fee_sw = df.column("ben_fee_sw").unwrap().str().unwrap().get(row).unwrap_or_else(|| "ben_fee_sw not found").to_string();
        let adjfee = df.column("adjfee").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjfee not found").to_string();
        let npcifee = df.column("npcifee").unwrap().str().unwrap().get(row).unwrap_or_else(|| "npcifee not found").to_string();
        let remfeetax = df.column("remfeetax").unwrap().str().unwrap().get(row).unwrap_or_else(|| "remfeetax not found").to_string();
        let benfeetax = df.column("benfeetax").unwrap().str().unwrap().get(row).unwrap_or_else(|| "benfeetax not found").to_string();
        let npcitax = df.column("npcitax").unwrap().str().unwrap().get(row).unwrap_or_else(|| "npcitax not found").to_string();
        let adjref = df.column("adjref").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjref not found").to_string();
        let bankadjref = df.column("bankadjref").unwrap().str().unwrap().get(row).unwrap_or_else(|| "bankadjref not found").to_string();
        let adjproof = df.column("adjproof").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjproof not found").to_string();
        let compensation_amount = df.column("compensation_amount").unwrap().str().unwrap().get(row).unwrap_or_else(|| "compensation_amount not found").to_string();
        let adjustment_raised_time = df.column("adjustment_raised_time").unwrap().str().unwrap().get(row).unwrap_or_else(|| "adjustment_raised_time not found").to_string();
        let no_of_days_for_penalty = df.column("no_of_days_for_penalty").unwrap().str().unwrap().get(row).unwrap_or_else(|| "no_of_days_for_penalty not found").to_string();
        let shdt73 = df.column("shdt73").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt73 not found").to_string();
        let shdt74 = df.column("shdt74").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt74 not found").to_string();
        let shdt75 = df.column("shdt75").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt75 not found").to_string();
        let shdt76 = df.column("shdt76").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt76 not found").to_string();
        let shdt77 = df.column("shdt77").unwrap().str().unwrap().get(row).unwrap_or_else(|| "shdt77 not found").to_string();
        let transaction_type = df.column("transaction_type").unwrap().str().unwrap().get(row).unwrap_or_else(|| "transaction_type not found").to_string();
        let transaction_indicator = df.column("transaction_indicator").unwrap().str().unwrap().get(row).unwrap_or_else(|| "transaction_indicator not found").to_string();
        let beneficiary_account_number = df.column("beneficiary_account_number").unwrap().str().unwrap().get(row).unwrap_or_else(|| "beneficiary_account_number not found").to_string();
        let remitter_account_number = df.column("remitter_account_number").unwrap().str().unwrap().get(row).unwrap_or_else(|| "remitter_account_number not found").to_string();
        let aadhar_number = df.column("aadhar_number").unwrap().str().unwrap().get(row).unwrap_or_else(|| "aadhar_number not found").to_string();
        let mobile_number = df.column("mobile_number").unwrap().str().unwrap().get(row).unwrap_or_else(|| "mobile_number not found").to_string();
        let payer_psp = df.column("payer_psp").unwrap().str().unwrap().get(row).unwrap_or_else(|| "payer_psp not found").to_string();
        let payee_psp = df.column("payee_psp").unwrap().str().unwrap().get(row).unwrap_or_else(|| "payee_psp not found").to_string();
        let upi_transaction_id = df.column("upi_transaction_id").unwrap().str().unwrap().get(row).unwrap_or_else(|| "upi_transaction_id not found").to_string();
        let virtual_address = df.column("virtual_address").unwrap().str().unwrap().get(row).unwrap_or_else(|| "virtual_address not found").to_string();
        let dispute_flag = df.column("dispute_flag").unwrap().str().unwrap().get(row).unwrap_or_else(|| "dispute_flag not found").to_string();
        let reason_code = df.column("reason_code").unwrap().str().unwrap().get(row).unwrap_or_else(|| "reason_code not found").to_string();
        let mcc = df.column("mcc").unwrap().str().unwrap().get(row).unwrap_or_else(|| "mcc not found").to_string();
        let originating_channel = df.column("originating_channel").unwrap().str().unwrap().get(row).unwrap_or_else(|| "originating_channel not found").to_string();

        new_models.push(NewYourModel::new(
            &id,
            &uid,
            &adjdate,
            &adjtype,
            &remitter,
            &beneficiery,
            &response,
            &txndate,
            &txntime,
            &rrn,
            &terminalid,
            &ben_mobile_no,
            &rem_mobile_no,
            &chbdate,
            &chbref,
            &txnamount,
            &adjamount,
            &rem_payee_psp_fee,
            &ben_fee,
            &ben_fee_sw,
            &adjfee,
            &npcifee,
            &remfeetax,
            &benfeetax,
            &npcitax,
            &adjref,
            &bankadjref,
            &adjproof,
            &compensation_amount,
            &adjustment_raised_time,
            &no_of_days_for_penalty,
            &shdt73,
            &shdt74,
            &shdt75,
            &shdt76,
            &shdt77,
            &transaction_type,
            &transaction_indicator,
            &beneficiary_account_number,
            &remitter_account_number,
            &aadhar_number,
            &mobile_number,
            &payer_psp,
            &payee_psp,
            &upi_transaction_id,
            &virtual_address,
            &dispute_flag,
            &reason_code,
            &mcc,
            &originating_channel,
        ));
    }

    Ok(new_models)
}

fn read_csv_to_updated_models(file_path: &str) -> Result<Vec<(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)>, Box<dyn std::error::Error>> {
    // Read CSV file and convert to updated models
    let update_reader = BufReader::new(File::open(file_path).expect("Failed to open file"));
    let update_df = CsvReader::new(update_reader)
        .infer_schema(Some(50))
        .has_header(true)
        .finish()
        .unwrap();

     // Get the columns
    let id_col = update_df.column("id").unwrap().str().unwrap();
    let uid_col = update_df.column("uid").unwrap().str().unwrap();
    let adjdate_col = update_df.column("adjdate").unwrap().str().unwrap();
    let adjtype_col = update_df.column("adjtype").unwrap().str().unwrap();
    let remitter_col = update_df.column("remitter").unwrap().str().unwrap();
    let beneficiery_col = update_df.column("beneficiery").unwrap().str().unwrap();
    let response_col = update_df.column("response").unwrap().str().unwrap();
    let txndate_col = update_df.column("txndate").unwrap().str().unwrap();
    let txntime_col = update_df.column("txntime").unwrap().str().unwrap();
    let rrn_col = update_df.column("rrn").unwrap().str().unwrap();
    let terminalid_col = update_df.column("terminalid").unwrap().str().unwrap();
    let ben_mobile_no_col = update_df.column("ben_mobile_no").unwrap().str().unwrap();
    let rem_mobile_no_col = update_df.column("rem_mobile_no").unwrap().str().unwrap();
    let chbdate_col = update_df.column("chbdate").unwrap().str().unwrap();
    let chbref_col = update_df.column("chbref").unwrap().str().unwrap();
    let txnamount_col = update_df.column("txnamount").unwrap().str().unwrap();
    let adjamount_col = update_df.column("adjamount").unwrap().str().unwrap();
    let rem_payee_psp_fee_col = update_df.column("rem_payee_psp_fee").unwrap().str().unwrap();
    let ben_fee_col = update_df.column("ben_fee").unwrap().str().unwrap();
    let ben_fee_sw_col = update_df.column("ben_fee_sw").unwrap().str().unwrap();
    let adjfee_col = update_df.column("adjfee").unwrap().str().unwrap();
    let npcifee_col = update_df.column("npcifee").unwrap().str().unwrap();
    let remfeetax_col = update_df.column("remfeetax").unwrap().str().unwrap();
    let benfeetax_col = update_df.column("benfeetax").unwrap().str().unwrap();
    let npcitax_col = update_df.column("npcitax").unwrap().str().unwrap();
    let adjref_col = update_df.column("adjref").unwrap().str().unwrap();
    let bankadjref_col = update_df.column("bankadjref").unwrap().str().unwrap();
    let adjproof_col = update_df.column("adjproof").unwrap().str().unwrap();
    let compensation_amount_col = update_df.column("compensation_amount").unwrap().str().unwrap();
    let adjustment_raised_time_col = update_df.column("adjustment_raised_time").unwrap().str().unwrap();
    let no_of_days_for_penalty_col = update_df.column("no_of_days_for_penalty").unwrap().str().unwrap();
    let shdt73_col = update_df.column("shdt73").unwrap().str().unwrap();
    let shdt74_col = update_df.column("shdt74").unwrap().str().unwrap();
    let shdt75_col = update_df.column("shdt75").unwrap().str().unwrap();
    let shdt76_col = update_df.column("shdt76").unwrap().str().unwrap();
    let shdt77_col = update_df.column("shdt77").unwrap().str().unwrap();
    let transaction_type_col = update_df.column("transaction_type").unwrap().str().unwrap();
    let transaction_indicator_col = update_df.column("transaction_indicator").unwrap().str().unwrap();
    let beneficiary_account_number_col = update_df.column("beneficiary_account_number").unwrap().str().unwrap();
    let remitter_account_number_col = update_df.column("remitter_account_number").unwrap().str().unwrap();
    let aadhar_number_col = update_df.column("aadhar_number").unwrap().str().unwrap();
    let mobile_number_col = update_df.column("mobile_number").unwrap().str().unwrap();
    let payer_psp_col = update_df.column("payer_psp").unwrap().str().unwrap();
    let payee_psp_col = update_df.column("payee_psp").unwrap().str().unwrap();
    let upi_transaction_id_col = update_df.column("upi_transaction_id").unwrap().str().unwrap();
    let virtual_address_col = update_df.column("virtual_address").unwrap().str().unwrap();
    let dispute_flag_col = update_df.column("dispute_flag").unwrap().str().unwrap();
    let reason_code_col = update_df.column("reason_code").unwrap().str().unwrap();
    let mcc_col = update_df.column("mcc").unwrap().str().unwrap();
    let originating_channel_col = update_df.column("originating_channel").unwrap().str().unwrap();
 
     // Perform bulk update
    let mut updated_models = Vec::new();
    for i in 0..id_col.len() {
        let id= id_col.get(i).unwrap().to_owned();
        let uid= uid_col.get(i).unwrap().to_owned();
        let adjdate= adjdate_col.get(i).unwrap().to_owned();
        let adjtype= adjtype_col.get(i).unwrap().to_owned();
        let remitter= remitter_col.get(i).unwrap().to_owned();
        let beneficiery= beneficiery_col.get(i).unwrap().to_owned();
        let response= response_col.get(i).unwrap().to_owned();
        let txndate= txndate_col.get(i).unwrap().to_owned();
        let txntime= txntime_col.get(i).unwrap().to_owned();
        let rrn= rrn_col.get(i).unwrap().to_owned();
        let terminalid= terminalid_col.get(i).unwrap().to_owned();
        let ben_mobile_no= ben_mobile_no_col.get(i).unwrap().to_owned();
        let rem_mobile_no= rem_mobile_no_col.get(i).unwrap().to_owned();
        let chbdate= chbdate_col.get(i).unwrap().to_owned();
        let chbref= chbref_col.get(i).unwrap().to_owned();
        let txnamount= txnamount_col.get(i).unwrap().to_owned();
        let adjamount= adjamount_col.get(i).unwrap().to_owned();
        let rem_payee_psp_fee= rem_payee_psp_fee_col.get(i).unwrap().to_owned();
        let ben_fee= ben_fee_col.get(i).unwrap().to_owned();
        let ben_fee_sw= ben_fee_sw_col.get(i).unwrap().to_owned();
        let adjfee= adjfee_col.get(i).unwrap().to_owned();
        let npcifee= npcifee_col.get(i).unwrap().to_owned();
        let remfeetax= remfeetax_col.get(i).unwrap().to_owned();
        let benfeetax= benfeetax_col.get(i).unwrap().to_owned();
        let npcitax= npcitax_col.get(i).unwrap().to_owned();
        let adjref= adjref_col.get(i).unwrap().to_owned();
        let bankadjref= bankadjref_col.get(i).unwrap().to_owned();
        let adjproof= adjproof_col.get(i).unwrap().to_owned();
        let compensation_amount= compensation_amount_col.get(i).unwrap().to_owned();
        let adjustment_raised_time= adjustment_raised_time_col.get(i).unwrap().to_owned();
        let no_of_days_for_penalty= no_of_days_for_penalty_col.get(i).unwrap().to_owned();
        let shdt73= shdt73_col.get(i).unwrap().to_owned();
        let shdt74= shdt74_col.get(i).unwrap().to_owned();
        let shdt75= shdt75_col.get(i).unwrap().to_owned();
        let shdt76= shdt76_col.get(i).unwrap().to_owned();
        let shdt77= shdt77_col.get(i).unwrap().to_owned();
        let transaction_type= transaction_type_col.get(i).unwrap().to_owned();
        let transaction_indicator= transaction_indicator_col.get(i).unwrap().to_owned();
        let beneficiary_account_number= beneficiary_account_number_col.get(i).unwrap().to_owned();
        let remitter_account_number= remitter_account_number_col.get(i).unwrap().to_owned();
        let aadhar_number= aadhar_number_col.get(i).unwrap().to_owned();
        let mobile_number= mobile_number_col.get(i).unwrap().to_owned();
        let payer_psp= payer_psp_col.get(i).unwrap().to_owned();
        let payee_psp= payee_psp_col.get(i).unwrap().to_owned();
        let upi_transaction_id= upi_transaction_id_col.get(i).unwrap().to_owned();
        let virtual_address= virtual_address_col.get(i).unwrap().to_owned();
        let dispute_flag= dispute_flag_col.get(i).unwrap().to_owned();
        let reason_code= reason_code_col.get(i).unwrap().to_owned();
        let mcc= mcc_col.get(i).unwrap().to_owned();
        let originating_channel = originating_channel_col.get(i).unwrap().to_owned();
        updated_models.push((id, uid, adjdate, adjtype, remitter, beneficiery, response, txndate, txntime, rrn, terminalid, ben_mobile_no, rem_mobile_no, chbdate, chbref, txnamount, adjamount, rem_payee_psp_fee, ben_fee, ben_fee_sw, adjfee, npcifee, remfeetax, benfeetax, npcitax, adjref, bankadjref, adjproof, compensation_amount, adjustment_raised_time, no_of_days_for_penalty, shdt73, shdt74, shdt75, shdt76, shdt77, transaction_type, transaction_indicator, beneficiary_account_number, remitter_account_number, aadhar_number, mobile_number, payer_psp, payee_psp, upi_transaction_id, virtual_address, dispute_flag, reason_code, mcc, originating_channel));
     }
    Ok(updated_models)
}

fn bulk_select_from_csv(file_path: &str, column_names: &[String]) -> Result<Vec<String>, Box<dyn Error>> {
    // Read CSV file and select the specified column
    let reader = BufReader::new(File::open(file_path)?);
    let df = CsvReader::new(reader)
        .infer_schema(Some(50))
        .has_header(true)
        .finish()
        .unwrap();

    let mut selected_data = Vec::new();

    // Select the first column found in the column_names list
    for column_name in column_names {
        if let Ok(selected_series) = df.column(column_name) {
            for row in 0..df.height() {
                let col_data = selected_series.get(row).map_or("".to_string(), |v| v.to_string());
                selected_data.push(col_data);
            }
            break; // Break once a column is found and selected
        }
    }

    Ok(selected_data)
}


