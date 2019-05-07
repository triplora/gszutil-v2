     O   05 CUSTOMER-RCD.
     IO    10 CUSTOMER-NAME.
             15 LAST-NAME    PIC X(16).
             15 FIRST-NAME   PIC X(16).
           10 CUSTOMER-HOME-ADDRESS.
             15 STREET       PIC X(30).
             15 CITY         PIC X(20).
             15 STATE        PIC X(2).
             15 ZIP          PIC X(10).
           10 CUSTOMER-PHONE-NUMBER    PIC X(10).
           10 CUSTOMER-CREDIT-LIMIT    PIC S9(6)V99.
           10 CUSTOMER-ACCT-BALANCE    PIC S9(13)V99 COMP-3
                          OCCURS 2 TIMES.
           10 CUSTOMER-INTEREST-RATE        COMP-1
                          OCCURS 2 TIMES.