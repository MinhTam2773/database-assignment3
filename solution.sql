/*******************
=========================================
CPRG-307 Assignment 3
Authors:
   - Minh Tam Nguyen
   - Mikael Ly
   - Xiaomei He
   - Elliot Jost

This is the solution file for Assignment 3 of Databases. 
=========================================
=========================================
Part 1 Requirements
=========================================
- Use explicit cursors to read from NEW_TRANSACTIONS [Implemented]

- Insert the read tables into TRANSACTION_DETAIL and TRANSACTION_HISTORY [Complete]

- Update the appropriate account balance in ACCOUNT [Implemented]
   - Determine Debit(D) or Credit(C) to decide whether to add or subtract. [Implemented]
   
- Removed processed transactions from NEW_TRANSACTIONS

- Include COMMIT to save changes
=========================================
Part 2 Requirements
=========================================
- Handle good + bad transactions

- Exception handling for both anticipated and unanticipated errors

- Error logging
   - Write descriptive error messages and transaction info into the WKIS_ERROR_LOG table

- Bad Transactions
   -   Remain in NEW_TRANSACTIONS (don't delete)
   - Should not update ACCOUNT, TRANSACTION_DETAIL, or TRANSACTION_HISTORY

- Valid Transactions
   - Remove processed transactions from NEW_TRANSACTIONS (same as part 1)

- Error Handling Rules
   - Only first error per transaction logged
   - Do not exit main loop on error, continue processing other transactions

- Only allowed hard coding 'C' and 'D' values as Constants [Complete]

=========================================
Part 2 Errors to handle
=========================================
- Missing Transaction Number (NULL transaction number) [Complete]
- Debits and Credits Not Equal (transaction imbalance)
- Invalid Account Number (account not found)
- Negative Transaction Amount
- Invalid Transaction Type (anything other than C or D)
- Unanticipated Errors [Complete(?)]


*******************/

   SET SERVEROUTPUT ON;

declare 
   -- Explicit Cursor for transaction id's
   cursor c_txn_ids is
   select distinct transaction_no
     from new_transactions;

   -- Explicit cursor for getting rows
   cursor c_txn_rows (
      p_txn_no new_transactions.transaction_no%type
   ) is
   select transaction_no,
          transaction_date,
          description,
          account_no,
          transaction_type,
          transaction_amount
     from new_transactions
    where transaction_no = p_txn_no
    order by account_no;

    -- Explicit cursor for getting null records
   cursor c_null_txn_rows is
   select transaction_no,
          transaction_date,
          description,
          account_no,
          transaction_type,
          transaction_amount
     from new_transactions
    where transaction_no is null;

    -- Constants

   g_credit       constant varchar2(1) := 'C'; -- constant for account type, represents Credit
   g_debit        constant varchar2(1) := 'D'; -- constant for account type, represents Debit

    -- Work Variables
   v_txn_no       new_transactions.transaction_no%type; -- the current transaction number being processed
   r_row          c_txn_rows%rowtype; -- a transaction row
   v_default_type account.default_type%type; -- the account's normal balance side, C or D
   v_balance      account.account_balance%type; -- the account's current balance
   v_first_date   new_transactions.transaction_date%type; -- holding var for the transaction_date of the current transaction
   v_first_desc   new_transactions.description%type; -- holding var for the description of the current transaction
   v_exists       number; -- the number of duplicates of a row in wkis_error_log
begin
   -- handle null transaction numbers
   -- ===============================
   open c_null_txn_rows; -- open the null transactions cursor
   fetch c_null_txn_rows into r_row;
   if c_null_txn_rows%found then
      v_first_date := r_row.transaction_date;
      v_first_desc := r_row.description;
   
   -- ensure null error entry is not duplicated (not already in the error log)
      select count(*)
        into v_exists
        from wkis_error_log
       where transaction_no is null
         and error_msg like 'Missing transaction number';

   -- insert error into log
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
      end if;
      dbms_output.put_line("Logged missing Transaction Number");
   end if;
   close c_null_txn_rows;
   -- ===============================

   -- Outer loop: Select distinct transaction number to process one transaction at a time
   -- process valid transactions
   open c_txn_ids;
   loop
      fetch c_txn_ids into v_txn_no;
      exit when c_txn_ids%notfound;

      -- open up embedded block to read in all rows and detect first error
      declare
         v_err_found     boolean;
         v_err_msg       varchar2(200);
         v_total_debits  number;
         v_total_credits number;
         v_first_date    date;
         v_first_desc    varchar2(200);
      begin
         v_err_found := false; -- whether an error has been found
         v_err_msg := null; -- error message
         v_total_debits := 0; -- total transaction in debits
         v_total_credits := 0; -- total transaction in credits
         v_first_date := null; -- current row's transaction date
         v_first_desc := null; -- current row's description

         -- open explicit cursor
         open c_txn_rows(v_txn_no);

         -- capture first row to keep header info available
         fetch c_txn_rows into r_row;
         if c_txn_rows%found then
            v_first_date := r_row.transaction_date;
            v_first_desc := r_row.description;

         -- Check for errors in this section
         -- ===============================
            -- If there hasn't been an error found, then check through for next error:
            if not v_err_found then
               -- Transaction Type error (transaction type is not D or C):
               if r_row.transaction_type not in ( g_debit,
                                                  g_credit ) then
                  v_err_found := true;
                  v_err_msg := 'Invalid Transaction Type '
                               || r_row.transaction_type
                               || ' at Transaction No. '
                               || v_txn_no;
               end if;

               -- If negative transaction amount:
               if
                  not v_err_found
                  and r_row.transaction_amount < 0
               then
                  v_err_found := true;
                  v_err_msg := 'Negative Transaction Amount '
                               || r_row.transaction_amount
                               || ' at Transaction No. '
                               || v_txn_no;
               end if;
               -- If invalid account number:
               if not v_err_found then
                  begin
                     select default_type,
                            account_balance
                       into
                        v_default_type,
                        v_balance
                       from account
                      where account_no = r_row.account_no;
                  exception
                     when no_data_found then
                        v_err_found := true;
                        v_err_msg := 'Invalid account number: '
                                     || r_row.account_no
                                     || ' for transaction '
                                     || v_txn_no;
                  end;
               end if;
               -- Accumulate totals (only for valid transaction types):
               if not v_err_found then
                  if r_row.transaction_type = g_debit then
                     v_total_debits := v_total_debits + r_row.transaction_amount;
                  else
                     v_total_credits := v_total_credits + r_row.transaction_amount;
                  end if;
               end if;
            end if;
         end if;
         -- Continue reading remaining rows and accumulate totals
         loop
            fetch c_txn_rows into r_row;
            exit when c_txn_rows%notfound;
            if not v_err_found then
            -- if invalid type
               if r_row.transaction_type not in ( g_debit,
                                                  g_credit ) then
                  v_err_found := true;
                  v_err_msg := 'Invalid transaction type: '
                               || r_row.transaction_type
                               || ' for transaction '
                               || v_txn_no;
               end if;
            end if;
         end loop;
         close c_txn_rows;
         -- ===============================
         -- Begin processing in this section, or log error
         -- ===============================
         -- If error, log only first error for this transaction number
         if v_err_found then
            select count(*)
              into v_exists
              from wkis_error_log
             where transaction_no = v_txn_no;

            -- If there are no duplicate entries in log, log the error
            if v_exists = 0 then
               insert into wkis_error_log (
                  transaction_no,
                  transaction_date,
                  description,
                  error_msg
               ) values ( v_txn_no,
                          v_first_date,
                          v_first_desc,
                          v_err_msg );
            end if;

            dbms_output.put_line('Error with Transaction No. '
                                 || v_txn_no
                                 || ': ' || v_err_msg);
         -- ===============================      
         -- If transaction is clean (no errors found), begin processing
         -- ===============================
         -- print processing info
         else
            dbms_output.put_line('Processing: transaction id: '
                                 || v_txn_no
                                 || ' amount: '
                                 || r_row.transaction_amount
                                 || ' type: ' || r_row.transaction_type);
            -- insert into transaction history
            insert into transaction_history (
               transaction_no,
               transaction_date,
               description
            ) values ( v_txn_no,
                       v_first_date,
                       v_first_desc );
            -- Re-open row cursor to insert details + update accounts
            open c_txn_rows(v_txn_no);
            loop
               fetch c_txn_rows into r_row;
               exit when c_txn_rows%notfound;

               -- Insert into table
               insert into transaction_detail (
                  transaction_no,
                  account_no,
                  transaction_type,
                  transaction_amount
               ) values ( r_row.transaction_no,
                          r_row.account_no,
                          r_row.transaction_type,
                          r_row.transaction_amount );
               
               -- Get account default + balance
               select default_type,
                      account_balance
                 into
                  v_default_type,
                  v_balance
                 from account
                where account_no = r_row.account_no;

                -- Apply balance rule: add when transaction type matches default otherwise subtract
               if v_default_type = r_row.transaction_type then
                  v_balance := v_balance + r_row.transaction_amount;
               else
                  v_balance := v_balance - r_row.transaction_amount;
               end if;
               -- update account balance
               update account
                  set
                  account_balance = v_balance
                where account_no = r_row.account_no;
            end loop; -- end of insertion/update loop
            close c_txn_rows;
            -- ===============================
            -- Remove from NEW_TRANSACTIONS 
            delete from new_transactions
             where transaction_no = v_txn_no;

         end if;
         
         -- ===============================

         end; -- end of embedded block

      -- Exception Handling, unanticipated errors
      -- ===============================
      exception
         when others then
            dbms_output.put_line('Error while processing transaction no '
                                 || v_txn_no
                                 || ': ' || v_err_msg);
            -- Insert error message into error log table, avoid duplicates
            select count(*)
              into v_exists
              from wkis_error_log
             where transaction_no = v_txn_no;

            -- If there are no duplicate entries in log, log the error
            if v_exists = 0 then
               insert into wkis_error_log (
                  transaction_no,
                  transaction_date,
                  description,
                  error_msg
               ) values ( v_txn_no,
                          v_first_date,
                          v_first_desc,
                          v_err_msg );
            end if;

            dbms_output.put_line(v_err_msg);
            -- ensure not to delete or commit partial changes for this transaction
            rollback;
      end;
      -- ===============================

   end loop; -- end of outer loop
   close c_txn_ids;
   -- commit to save changes
   -- COMMIT;
end;
/