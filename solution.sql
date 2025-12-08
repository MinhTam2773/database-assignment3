/************************************************************
CPRG-307 – Database Programming
Assignment 3 – Transaction Processing System
Authors:
   - Minh Tam Nguyen
   - Mikael Ly
   - Xiaomei He
   - Elliot Jost

Description:
This PL/SQL program processes financial transactions stored in
the NEW_TRANSACTIONS table. It implements all Assignment 3
requirements, including:

Part 1 – Valid Transaction Processing
-------------------------------------
Uses explicit cursors to retrieve transaction numbers and related rows.
Inserts valid transaction data into TRANSACTION_DETAIL and TRANSACTION_HISTORY.
Updates account balances in the ACCOUNT table using the appropriate debit/credit logic.
Removes successfully processed transactions from NEW_TRANSACTIONS.
Commits all valid changes.

Part 2 – Error Detection & Logging
----------------------------------
Detects and handles invalid transactions without stopping the main loop.
Supports the following error conditions:
     - Missing transaction number
     - Invalid transaction type (must be 'D' or 'C')
     - Negative transaction amount
     - Invalid account number (no matching ACCOUNT record)
     - Debit/Credit imbalance within a transaction number
     - Unanticipated runtime errors
Logs only the first error per transaction to WKIS_ERROR_LOG.
Leaves erroneous transactions in NEW_TRANSACTIONS.
Ensures no partial updates occur for invalid transactions.

*************************************************************/

   SET SERVEROUTPUT ON;

declare
   g_credit        constant varchar2(1) := 'C';
   g_debit         constant varchar2(1) := 'D';
   v_first_date    new_transactions.transaction_date%type;
   v_first_desc    new_transactions.description%type;
   v_total_debits  number;
   v_total_credits number;
   v_err_found     boolean;
   v_err_msg       varchar2(200);
   v_exists        number;
begin
  -- Outer: iterate distinct transaction numbers (will include NULL once if present)
   for txn_rec in (
      select distinct transaction_no
        from new_transactions
   ) loop
      begin
    -- initialize per-transaction vars
         v_err_found := false;
         v_err_msg := null;
         v_total_debits := 0;
         v_total_credits := 0;
         v_first_date := null;
         v_first_desc := null;

    -- Handle NULL transaction number explicitly: log once and skip processing rows
         if txn_rec.transaction_no is null then
      -- fetch one row to get date/desc for error message 
            for null_row in (
               select transaction_date,
                      description
                 from new_transactions
                where transaction_no is null
                  and rownum = 1
            ) loop
               v_first_date := null_row.transaction_date;
               v_first_desc := null_row.description;
            end loop;

            select count(*)
              into v_exists
              from wkis_error_log
             where transaction_no is null
               and error_msg = 'Missing transaction number';

            if v_exists = 0 then
               insert into wkis_error_log (
                  transaction_no,
                  transaction_date,
                  description,
                  error_msg
               ) values ( null,
                          v_first_date,
                          v_first_desc,
                          'Missing transaction number' );
               dbms_output.put_line('Logged missing Transaction Number');
            end if;

      -- skip to next transaction_no
            continue;
         end if;

    -- Inner: iterate all rows for this transaction
         for row_rec in (
            select transaction_no,
                   transaction_date,
                   description,
                   account_no,
                   transaction_type,
                   transaction_amount
              from new_transactions
             where transaction_no = txn_rec.transaction_no
             order by account_no
         ) loop
      -- capture header info from first row
            if v_first_date is null then
               v_first_date := row_rec.transaction_date;
               v_first_desc := row_rec.description;
            end if;

      -- validate transaction type
            if not v_err_found then
               if row_rec.transaction_type not in ( g_debit,
                                                    g_credit ) then
                  v_err_found := true;
                  v_err_msg := 'Invalid transaction type '
                               || row_rec.transaction_type
                               || ' for transaction '
                               || txn_rec.transaction_no;
               elsif row_rec.transaction_amount < 0 then
                  v_err_found := true;
                  v_err_msg := 'Negative amount '
                               || row_rec.transaction_amount
                               || ' for transaction '
                               || txn_rec.transaction_no;
               else
          -- validate account exists and get default type/balance
                  begin
                     declare
                        v_default_type account_type.default_trans_type%type;
                        v_balance      account.account_balance%type;
                     begin
                        select at.default_trans_type,
                               a.account_balance
                          into
                           v_default_type,
                           v_balance
                          from account a
                          join account_type at
                        on a.account_type_code = at.account_type_code
                         where a.account_no = row_rec.account_no;
              -- no immediate action here; we just checked existence
                     end;
                  exception
                     when no_data_found then
                        v_err_found := true;
                        v_err_msg := 'Invalid account number: '
                                     || row_rec.account_no
                                     || ' for transaction '
                                     || txn_rec.transaction_no;
                  end;
               end if;
            end if;

      -- accumulate totals if still valid
            if not v_err_found then
               if row_rec.transaction_type = g_debit then
                  v_total_debits := v_total_debits + row_rec.transaction_amount;
               else
                  v_total_credits := v_total_credits + row_rec.transaction_amount;
               end if;
            end if;

         end loop; -- inner row loop

    -- After reading all rows, check debits == credits
         if not v_err_found then
            if v_total_debits <> v_total_credits then
               v_err_found := true;
               v_err_msg := 'Debits ('
                            || v_total_debits
                            || ') not equal to Credits ('
                            || v_total_credits
                            || ') for transaction '
                            || txn_rec.transaction_no;
            end if;
         end if;

    -- If error, insert error log once
         if v_err_found then
            select count(*)
              into v_exists
              from wkis_error_log
             where transaction_no = txn_rec.transaction_no;

            if v_exists = 0 then
               insert into wkis_error_log (
                  transaction_no,
                  transaction_date,
                  description,
                  error_msg
               ) values ( txn_rec.transaction_no,
                          v_first_date,
                          v_first_desc,
                          v_err_msg );
            end if;

            dbms_output.put_line('Error for txn '
                                 || txn_rec.transaction_no
                                 || ': ' || v_err_msg);

      -- do not delete rows for this transaction; move to next txn
            continue;
         end if;

    -- If clean: insert transaction_history, insert details, update accounts, delete from new_transactions
    -- re-loop over the rows for insertion/update
         insert into transaction_history (
            transaction_no,
            transaction_date,
            description
         ) values ( txn_rec.transaction_no,
                    v_first_date,
                    v_first_desc );

         for row_rec in (
            select transaction_no,
                   account_no,
                   transaction_type,
                   transaction_amount
              from new_transactions
             where transaction_no = txn_rec.transaction_no
         ) loop
            insert into transaction_detail (
               transaction_no,
               account_no,
               transaction_type,
               transaction_amount
            ) values ( row_rec.transaction_no,
                       row_rec.account_no,
                       row_rec.transaction_type,
                       row_rec.transaction_amount );

      -- adjust account
            declare
               v_default_type account_type.default_trans_type%type;
               v_balance      account.account_balance%type;
            begin
               select at.default_trans_type,
                      a.account_balance
                 into
                  v_default_type,
                  v_balance
                 from account a
                 join account_type at
               on a.account_type_code = at.account_type_code
                where a.account_no = row_rec.account_no;

               if v_default_type = row_rec.transaction_type then
                  v_balance := v_balance + row_rec.transaction_amount;
               else
                  v_balance := v_balance - row_rec.transaction_amount;
               end if;

               update account
                  set
                  account_balance = v_balance
                where account_no = row_rec.account_no;
            end;
         end loop;

    -- finally delete processed rows
         delete from new_transactions
          where transaction_no = txn_rec.transaction_no;

      --unexpected error
      exception
         when others then
      -- log system error into wkis_error_log once for this transaction (avoid duplicates)
            v_err_msg := sqlerrm;
            select count(*)
              into v_exists
              from wkis_error_log
             where transaction_no = txn_rec.transaction_no;
            if v_exists = 0 then
               insert into wkis_error_log (
                  transaction_no,
                  transaction_date,
                  description,
                  error_msg
               ) values ( txn_rec.transaction_no,
                          v_first_date,
                          v_first_desc,
                          v_err_msg );
            end if;
            dbms_output.put_line('Unhandled error for txn '
                                 || txn_rec.transaction_no
                                 || ': ' || v_err_msg);
      -- ensure we don't propagate failure; move to next transaction
            continue;
      end;
   end loop; -- outer txn loop

   commit;
end;
/