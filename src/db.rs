use diesel::dsl::sql;
use diesel::prelude::*;
use diesel::pg::PgConnection;
use diesel::sql_types::Text;
use dotenv::dotenv;
use std::env;
use crate::models::{MyTable,NewYourModel};
use crate::schema::my_table::dsl::*;

pub fn establish_connection() -> PgConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set");
    println!("Database URL: {}", database_url);

    PgConnection::establish(&database_url)
        .expect(&format!("Error connecting to {}", database_url))
}

pub fn bulk_insert_your_models(connection: &mut PgConnection, new_models: Vec<NewYourModel>, batch_size: usize) {
    let chunks = new_models.chunks(batch_size);
    for chunk in chunks {
      diesel::insert_into(my_table)
        .values(chunk)
        .execute(connection)
        .expect("Error inserting rows");
    }
  }

pub fn bulk_update_your_models(connection: &mut PgConnection, updated_models: Vec<(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)>) {
    for (id_value, uid_value, adjdate_value, adjtype_value, remitter_value, beneficiery_value, response_value, txndate_value, txntime_value, rrn_value, terminalid_value, ben_mobile_no_value, rem_mobile_no_value, chbdate_value, chbref_value, txnamount_value, adjamount_value, rem_payee_psp_fee_value, ben_fee_value, ben_fee_sw_value, adjfee_value, npcifee_value, remfeetax_value, benfeetax_value, npcitax_value, adjref_value, bankadjref_value, adjproof_value, compensation_amount_value, adjustment_raised_time_value, no_of_days_for_penalty_value, shdt73_value, shdt74_value, shdt75_value, shdt76_value, shdt77_value, transaction_type_value, transaction_indicator_value, beneficiary_account_number_value, remitter_account_number_value, aadhar_number_value, mobile_number_value, payer_psp_value, payee_psp_value, upi_transaction_id_value, virtual_address_value, dispute_flag_value, reason_code_value, mcc_value, originating_channel_value) in updated_models {
        diesel::update(my_table.find(id_value))
            .set((uid.eq(uid_value), adjdate.eq(adjdate_value), adjtype.eq(adjtype_value), remitter.eq(remitter_value), beneficiery.eq(beneficiery_value), response.eq(response_value), txndate.eq(txndate_value), txntime.eq(txntime_value), rrn.eq(rrn_value), terminalid.eq(terminalid_value), ben_mobile_no.eq(ben_mobile_no_value), rem_mobile_no.eq(rem_mobile_no_value), chbdate.eq(chbdate_value), chbref.eq(chbref_value), txnamount.eq(txnamount_value), adjamount.eq(adjamount_value), rem_payee_psp_fee.eq(rem_payee_psp_fee_value), ben_fee.eq(ben_fee_value), ben_fee_sw.eq(ben_fee_sw_value), adjfee.eq(adjfee_value), npcifee.eq(npcifee_value), remfeetax.eq(remfeetax_value), benfeetax.eq(benfeetax_value), npcitax.eq(npcitax_value), adjref.eq(adjref_value), bankadjref.eq(bankadjref_value), adjproof.eq(adjproof_value), compensation_amount.eq(compensation_amount_value), adjustment_raised_time.eq(adjustment_raised_time_value), no_of_days_for_penalty.eq(no_of_days_for_penalty_value), shdt73.eq(shdt73_value), shdt74.eq(shdt74_value), shdt75.eq(shdt75_value), shdt76.eq(shdt76_value), shdt77.eq(shdt77_value), transaction_type.eq(transaction_type_value), transaction_indicator.eq(transaction_indicator_value), beneficiary_account_number.eq(beneficiary_account_number_value), remitter_account_number.eq(remitter_account_number_value), aadhar_number.eq(aadhar_number_value), mobile_number.eq(mobile_number_value), payer_psp.eq(payer_psp_value), payee_psp.eq(payee_psp_value), upi_transaction_id.eq(upi_transaction_id_value), virtual_address.eq(virtual_address_value), dispute_flag.eq(dispute_flag_value), reason_code.eq(reason_code_value), mcc.eq(mcc_value), originating_channel.eq(originating_channel_value)))
            .execute(connection)
            .expect("Error updating row");
    }
}

pub fn bulk_select_your_models(connection: &mut PgConnection, column_name: &str, mut column_values: Vec<String>) -> Vec<MyTable> {
    column_values.iter_mut().for_each(|value| {
        *value = value.replace("\"", "");
    });
    let column = sql::<Text>(column_name);
    my_table
        .filter(column.eq_any(column_values))
        .load::<MyTable>(connection)
        .expect("Error loading your models")
}


pub fn select_specific_column(connection: &mut PgConnection, select_column: &str, column_name: &str, mut column_values: Vec<String>) -> Vec<String> {
    column_values.iter_mut().for_each(|value| {
        *value = value.replace("\"", "");
    });
    let column = sql::<Text>(column_name);
    let column_name = sql::<Text>(select_column);
    my_table
        .select(column_name)
        .filter(column.eq_any(column_values))
        .load::<String>(connection)
        .expect("Error loading id")
}