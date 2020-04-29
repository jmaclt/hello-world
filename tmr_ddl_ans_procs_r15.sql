/*<TOAD_FILE_CHUNK>*/

-- ################################################################
-- ### GENERIC MODULE NAME: 		tmr_ddl_procs.sql            
-- ### INTERNAL VERSION: 	        15
-- ### RELEASE (AFFILIATE, PATCH#):	Sweden, 15_881
-- ### OWNER SCHEMA: 			    ANS_PROCS
-- ### OWNER INTERFACE: 		    TAMRO
-- ### OWNER SUBSYSTEM:             INVENTORY
-- ### PURPOSE: 
-- ### Package containing the programs for the TAMRO interface.
-- ### This interface loads the following data items from a daily
-- ### text file provided by Tamro AB (Sweden).
-- ### 
CREATE OR REPLACE PACKAGE Pck_Tamro_Interface AS

/*
==================================================================
 PACKAGE SPECIFICATION 
==================================================================
*/

/* ------------------------------------------------------------------
Package Name    : PCK_Tamro_Interface 

Project         : AnSWERS, Interface with TAMRO data source.
                  Country: SWEDEN  

Module          : -

Purpose         : Load TAMRO data into AnSWERS data warehouse
                  PLEASE REFER TO THE RELATED "IMPLEMENTATION NOTES"
                  DOCUMENT. 

Comments        : Based on the specification document 
                  "Tamro (SIRS) Loading Specification"
                  Author: Didier Deraedt
                  
                  IMPORTANT NOTE: The load procedure assumes
                  that the flat table TMR_TAMRO is loaded using 
                  REPLACE/TRUNCATE mode (ie, it is filled anew in each load job). 

Database Schema : ANS_PROCS 

Public Procedures: 

   * Load_CifPrices()	     -- Load CIF prices
                             -- THIS PROCEDURE SHOULD BE THE FIRST TO RUN	
							 						 
   * Load_Lots()             -- Load Lot data
                             -- THIS PROCEDURE RUN BEFORE C. STOCKS AND C. SALES
			   
   * Load_Consig_Stocks()    -- Load Stock data 
   
   * Load_Consig_Sales()     -- Load Consignment Sales data
                             -- THIS PROCEDURE SHOULD RUN ONLY AFTER Load_Consig_Stocks()
   
   * Load_Street_Sales(reload_mode)    -- Load Street Sales data
                                       -- make reload_mode = 'AFTER_MXRF' if this is
									   a reload of the same TAMRO records after having 
									   manual xref'd the customer Ids.
      
   * Load_Steward_Screens()  -- Load SAD auxiliary tables used in stewardship screens.
                             -- THIS PROCEDURE SHOULD BE THE LAST TO RUN
                             
   * Unload(mode)            -- Delete all data inserted by TAMRO interface 
   
Modified Tables : 
(other than the common ones like System_Events, Xref, etc) 
---------------------------------------------------------
EDM: 
   Edm_Outgng_Shpmnt 
   Edm_Cust_Ordr
   Edm_Cust_Ordr_Line 
   Edm_Lot
   Edm_Outgng_Shpmnt_Line 
   Edm_Acqrd_Sales_Data
   Edm_Invntry_Lot_Prtn
   Edm_Invntry_Non_Lot
   Edm_Item_Prc
   
EXTENSIONS - ANS_CORE_EXT_OWNER:
   Ext_Invntry_Mvmnt 

Log Tables (SAD): 
   Tmr_Tamro_Log 
   Tmr_Outgng_Shpmnt_Log 
   Tmr_Cust_Ordr_Log 
   Tmr_Cust_Ordr_Line_Log 
   Tmr_Lot_Log 
   Tmr_Outgng_Shpmnt_Line_Log 
   Tmr_Acqrd_Sales_Data_Log   
   Tmr_Ext_Invntry_Mvmnt_Log 
   Tmr_Invntry_Lot_Prtn_Log
   Tmr_Invntry_Non_Lot_Log
   
Auxiliary Tables (SAD): 
   Tmr_Consignment_Sales 
   Tmr_Consignment_Stock 
   Tmr_Street_Sales 
   Tmr_Translation [READ ONLY]
   Tmrs_Customer_Sirs [READ ONLY]
   Tmrs_Mainscreen (used by the XREF screen)

   NOTE: The first 3 tables are emptied at the begining
   of each run.
    
Flat Tables (Flat):  
   Tmr_Tamro 
   
   NOTE: Invalid records (headers/footers) deleted from the flat table
   but are not inserted in the log table anymore (20-Oct-2003). 


Copyright	: Eli Lilly and Company, 2003


Author		: LOURENCO_JOAO_MIGUEL@LILLY.COM  

Version		: 15

Creation Date   : 3-July-2003 

QAR Date        : 
                               
                              
------------------------------------------------------------------*/



/* ------------------ Public Procedures ------------------------ */


PROCEDURE Load_CifPrices;

PROCEDURE Load_Lots;

PROCEDURE Load_Consig_Stocks;

PROCEDURE Load_Consig_Sales;

PROCEDURE Load_Street_Sales (reload_after_manual_xrf_ IN VARCHAR2 DEFAULT 'first');

PROCEDURE Load_Steward_Screens;

PROCEDURE Unload (exclude_ssal_ IN VARCHAR2 DEFAULT 'NO');

END Pck_Tamro_Interface;
/
/*<TOAD_FILE_CHUNK>*/

CREATE OR REPLACE PACKAGE BODY Pck_Tamro_Interface AS

/*************************************************************************
                         PRIVATE DATA TYPES
**************************************************************************/
TYPE List_Tables IS VARRAY(10) OF VARCHAR2(30);
TYPE List_Keys   IS VARRAY(5)  OF VARCHAR2(60);


/*************************************************************************
                          GLOBAL PRIVATE VARIABLES
**************************************************************************/
SRC_SYS_          CONSTANT Xrf_Xreference.Xref_Source_System%TYPE := 'TAMRO';
SRC_TAB_          CONSTANT Xrf_Xreference.Xref_Source_Table%TYPE  := 'TMR_TAMRO'; 
MODULE_NM_        System_Events.Module_Name%TYPE;
FLOW_NM_          VARCHAR2(10);
LD_ID_            Ans_Batch.Batch_Id%TYPE;
LD_DT_            DATE;
LD_USR_           VARCHAR2(30);
PRG_ERRORS_       BINARY_INTEGER := 0;
INVALID_REMOVED_  BOOLEAN := FALSE;
SWE_EDM_FCLTY_ID_ NUMBER := 9999;   

TAMRO_SWEDEN_NAME_ CONSTANT Ans_Edm_Owner.Edm_Org.Bsns_Nm%TYPE := 'Tamro AB';

-- Note: use cursor records as global variables to improve 
-- performance (avoid passing records as parameters to-and-from sub-programs)
rec_Csales            Ans_Sad_Owner.Tmr_Consignment_Sales%ROWTYPE;
rec_Ssales            Ans_Sad_Owner.Tmr_Street_Sales%ROWTYPE;



/*************************************************************************
                          PRIVATE SUBPROGRAMS
**************************************************************************/

/* -------------------------------------------------------------------------- 
Exists_Xref - format 1 (full parameter list) 
-------------------------------------------------------------------------- */
PROCEDURE Exists_Xref ( 
   source_sys_     IN   Xrf_Xreference.Xref_Source_System%TYPE,
   source_tab_     IN   Xrf_Xreference.Xref_Source_Table%TYPE,
   source_key_     IN   Xrf_Xreference.Xref_Source_Key%TYPE,
   edm_tab_        IN   Xrf_Xreference.Xref_Answers_Table%TYPE,
   edm_id_         OUT  Xrf_Xreference.Xref_Answers_Id%TYPE,
   result_         OUT  BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_EX1';
   edm_id_     := NULL;

   SELECT /*+ FIRST_ROWS(1) */ Xref_Answers_Id
   INTO edm_id_ 
   FROM Xrf_Xreference
   WHERE  Xref_Source_System =  source_sys_
     AND  Xref_Source_Table  =  UPPER(source_tab_)
     AND  Xref_Source_Key    =  source_key_
     AND  Hold_Flag          IN ('V','L')
     AND  Xref_Answers_Table =  UPPER(edm_tab_)
     AND  ROWNUM = 1;
	 
   result_ := 1;  -- found 
   RETURN;

EXCEPTION
   WHEN NO_DATA_FOUND THEN 
      result_     := 0; -- not found 
      edm_id_     := NULL;
	  RETURN;

END Exists_Xref;


/* -------------------------------------------------------------------------- 
Exists_Xref - format 2 (does not use Source_Table parameter) 
-------------------------------------------------------------------------- */
PROCEDURE Exists_Xref ( 
   source_sys_     IN   Xrf_Xreference.Xref_Source_System%TYPE,
   source_key_     IN   Xrf_Xreference.Xref_Source_Key%TYPE,
   edm_tab_        IN   Xrf_Xreference.Xref_Answers_Table%TYPE,
   edm_id_         OUT  Xrf_Xreference.Xref_Answers_Id%TYPE,
   result_         OUT  BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_EX2';
   edm_id_     := NULL;
    
   SELECT /*+ FIRST_ROWS(1) */ Xref_Answers_Id
   INTO edm_id_ 
   FROM Xrf_Xreference
   WHERE  Xref_Source_System =  source_sys_
     AND  Xref_Source_Key    =  source_key_
     AND  Hold_Flag          IN ('V','L')
     AND  Xref_Answers_Table =  UPPER(edm_tab_)
     AND  ROWNUM = 1;
	 
   result_ := 1;  -- found 
   RETURN;

EXCEPTION
   WHEN NO_DATA_FOUND THEN 
      result_     := 0; -- not found 
      edm_id_     := NULL;
	  RETURN;

END Exists_Xref;


/* -------------------------------------------------------------------------- 
Insert_Xref
-------------------------------------------------------------------------- */
FUNCTION Insert_Xref ( 
   source_sys_     IN   Xrf_Xreference.Xref_Source_System%TYPE,
   source_tab_     IN   Xrf_Xreference.Xref_Source_Table%TYPE,
   source_key_     IN   Xrf_Xreference.Xref_Source_Key%TYPE,
   log_tab_        IN   Xrf_Xreference.Xref_Log_Table%TYPE,
   edm_tab_        IN   Xrf_Xreference.Xref_Answers_Table%TYPE,
   edm_id_         IN   Xrf_Xreference.Xref_Answers_Id%TYPE,
   hold_flg_       IN   Xrf_Xreference.Hold_Flag%TYPE )
RETURN BOOLEAN IS
BEGIN
   MODULE_NM_   := 'TMR_IX';

   INSERT INTO Xrf_Xreference
   (
      Xref_Id,
      Xref_Source_System,
      Xref_Source_Key,
      Xref_Log_Table, 
      Xref_Answers_Table,
      Xref_Answers_Id,
  	  Hold_Flag,
  	  Date_Created,
  	  -- Date_Modified 
  	  User_Created_By,
  	  -- User_Modified_By 
  	  Xref_Source_Table
   )
   VALUES
   (
      Xrf_Xreference_Seq.NEXTVAL,
	  source_sys_,
	  source_key_,
	  log_tab_,
	  edm_tab_,
	  edm_id_,
	  hold_flg_,
	  LD_DT_,
   	  -- Date_Modified,
	  LD_USR_,
	  -- User_Modified_By,
	  source_tab_ 
   );
   RETURN TRUE;

EXCEPTION
   WHEN OTHERS THEN
      -- event_log.log_event(SQLERRM, MODULE_NM_, 'E'); 
      RETURN FALSE;

END Insert_Xref;


/*-------------------------------------------------------------------------- 
Validate_Xref_Flag
--------------------------------------------------------------------------*/
PROCEDURE Validate_Xref_Flag (
   edm_table_          IN VARCHAR2,
   edm_id_             IN NUMBER,
   hold_flag_          IN Xrf_Xreference.Hold_Flag%TYPE ) IS

BEGIN
   MODULE_NM_  := 'TMR_VXF';
   
   UPDATE Xrf_Xreference
   SET Hold_Flag = hold_flag_ 
   WHERE 
         Xref_Answers_Table = edm_table_
     AND Xref_Answers_Id    = edm_id_;
   
EXCEPTION
   WHEN OTHERS THEN RETURN;
      
END Validate_Xref_Flag;


/* -------------------------------------------------------------------------- 
CheckMand_Edm_Lot 
-------------------------------------------------------------------------- */  
FUNCTION CheckMand_Edm_Lot( 
           expiry_date_   IN            Ans_Flat_Owner.Tmr_Tamro.Expiry_Date%TYPE,
           r_Lot          IN OUT NOCOPY Edm_Lot%ROWTYPE ) 
RETURN BOOLEAN IS
BEGIN

   MODULE_NM_ := 'TMR_CMEL';
   r_Lot.Lot_Expry_Dt := NULL;
   r_Lot.Prchsd_Flg   := 0;   
   
-- Fill in the required mandatory fields of Edm_Lot record 

   IF expiry_date_ IS NULL OR expiry_date_ LIKE '0%' THEN
      RETURN FALSE;
   ELSE
      r_Lot.Lot_Expry_Dt := TO_DATE(expiry_date_, 'YYYYMMDD');
      RETURN TRUE;
   END IF;
   
EXCEPTION
   WHEN OTHERS THEN RETURN FALSE;
      
      
END CheckMand_Edm_Lot;


/*-------------------------------------------------------------------------- 
CheckMand_Edm_Cust_Ordr
--------------------------------------------------------------------------*/  
FUNCTION CheckMand_Edm_Cust_Ordr( r IN OUT NOCOPY Edm_Cust_Ordr%ROWTYPE ) 
RETURN BOOLEAN IS
BEGIN

   MODULE_NM_  := 'TMR_CMECO';
   r.Cstmr_Ordr_Nbr := NULL; -- it will be filled with the value of EDM_ID later on
   r.Ordr_Dt        := NULL;
   
-- Fill in the required mandatory fields of Edm_Cust_Ordr table 
   
   -- 6.2
   r.Ordr_Dt        := TO_DATE(TRIM(rec_Csales.Transaction_Date), 'YYYYMMDD'); 
   
   RETURN TRUE;
   
EXCEPTION
   WHEN OTHERS THEN RETURN FALSE;
            
END CheckMand_Edm_Cust_Ordr;


/*-------------------------------------------------------------------------- 
CheckMand_Edm_Outgng_Shpmnt
--------------------------------------------------------------------------*/  
FUNCTION CheckMand_Edm_Outgng_Shpmnt( r IN OUT NOCOPY Edm_Outgng_Shpmnt%ROWTYPE ) 
RETURN BOOLEAN IS
BEGIN

   MODULE_NM_  := 'TMR_CMEOS';

   r.Outgng_Shpmnt_Id := NULL;

-- Fill in the required mandatory fields of Edm_Outgng_Shpmnt table 
   r.Exprtr_Ein_Nbr   := rec_Csales.Transaction_Date;
   r.Shpmnt_Dt        := TO_DATE(TRIM(rec_Csales.Transaction_Date), 'YYYYMMDD'); -- 6.2
   
   RETURN TRUE;
 
EXCEPTION
   WHEN OTHERS THEN RETURN FALSE;
            
END CheckMand_Edm_Outgng_Shpmnt;



/*-------------------------------------------------------------------------- 
CheckMand_Edm_Cust_Ordr_Line
--------------------------------------------------------------------------*/  
FUNCTION CheckMand_Edm_Cust_Ordr_Line( r IN OUT NOCOPY Edm_Cust_Ordr_Line%ROWTYPE ) 
RETURN BOOLEAN IS
BEGIN

   MODULE_NM_  := 'TMR_CMECOL';
      
   r.Cstmr_Ordr_Line_Nbr := NULL; -- filled with EDM_ID later on
   
-- Fill in the required mandatory fields of Edm_Cust_Ordr_Line table 
-- (in this case they are not mandatory)

   r.Uom_Cd := 'PACKS';

   IF rec_Csales.Qty IS NULL THEN
      r.Ordr_Qty := NULL;
      RETURN FALSE;
   ELSE
      r.Ordr_Qty := rec_Csales.Qty;
   END IF;
   
   r.Lst_Prc_Amt  := rec_Csales.List_Price_Purchase;
   -- 6.0: r.Net_Slng_Amt := rec_Csales.Net_Value;
   r.Net_Slng_Amt      := rec_Csales.Qty*rec_Csales.List_Price_Purchase;
   r.Net_Slng_Amt_Euro := rec_Csales.Qty*rec_Csales.List_Price_Sales;
   
   RETURN TRUE;
    
EXCEPTION
   WHEN OTHERS THEN RETURN FALSE;
            
END CheckMand_Edm_Cust_Ordr_Line;


/*-------------------------------------------------------------------------- 
CheckMand_EdmOutgngShpmntLine 
--------------------------------------------------------------------------*/  
FUNCTION CheckMand_EdmOutgngShpmntLine( r IN OUT NOCOPY Edm_Outgng_Shpmnt_Line%ROWTYPE ) 
RETURN BOOLEAN IS
BEGIN

   MODULE_NM_  := 'TMR_CMEOSL';
   
   r.Shp_Qty                := NULL;
   r.Outgng_Shpmnt_Line_Nbr := NULL; -- filled with EDM_ID later on
    
-- Fill in the required mandatory fields of "r" record

   r.Uom_Cd := 'PACKS';   
    
   IF rec_Csales.Qty IS NULL THEN
      RETURN FALSE;
   ELSE
      r.Shp_Qty := rec_Csales.Qty;
   END IF;

   RETURN TRUE;
   
EXCEPTION
   WHEN OTHERS THEN RETURN FALSE;
          
END CheckMand_EdmOutgngShpmntLine;



/*-------------------------------------------------------------------------- 
CheckMand_Edm_Invntry_Lot_Prtn
--------------------------------------------------------------------------*/  
FUNCTION CheckMand_Edm_Invntry_Lot_Prtn( 
   t_qty      IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Qty%TYPE,
   t_date     IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Transaction_Date%TYPE, 
   r          IN OUT NOCOPY Edm_Invntry_Lot_Prtn%ROWTYPE ) 
   
RETURN BOOLEAN IS
BEGIN

   MODULE_NM_  := 'TMR_CMEIL';
   
   r.Loc_Desc               := 'Unknown';
       
-- Fill in the required mandatory fields of "r" record 
    
   IF t_qty IS NULL THEN
      RETURN FALSE; 
   END IF;
  
   IF t_date IS NULL OR t_date LIKE '0%' THEN   
      r.Invntry_As_At_Dt  := NULL;   
      RETURN FALSE;
   ELSE
      r.Invntry_As_At_Dt  := TO_DATE(t_date, 'YYYYMMDD');
   END IF;
   
   RETURN TRUE;
   
EXCEPTION
   WHEN OTHERS THEN RETURN FALSE;
   
END CheckMand_Edm_Invntry_Lot_Prtn;



/*-------------------------------------------------------------------------- 
CheckMand_Edm_Invntry_Non_Lot
--------------------------------------------------------------------------*/  
FUNCTION CheckMand_Edm_Invntry_Non_Lot(
   t_qty      IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Qty%TYPE,
   t_date     IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Transaction_Date%TYPE,    
   r          IN OUT NOCOPY Edm_Invntry_Non_Lot%ROWTYPE )
     
RETURN BOOLEAN IS
BEGIN

   MODULE_NM_  := 'TMR_CMEINL';
   
   r.Loc_Desc               := 'Unknown';
   r.Invntry_Sts            := 'A';
       
-- Fill in the required mandatory fields of "r" record 
    
   IF t_qty IS NULL THEN
      RETURN FALSE;
   END IF;
  
   IF t_date IS NULL OR t_date LIKE '0%' THEN  
      r.Invntry_As_At_Dt       := NULL;    
      RETURN FALSE;
   ELSE
      r.Invntry_As_At_Dt  := TO_DATE(t_date, 'YYYYMMDD');
   END IF;
   
   RETURN TRUE;
   
EXCEPTION
   WHEN OTHERS THEN RETURN FALSE;
   
END CheckMand_Edm_Invntry_Non_Lot;



/* -------------------------------------------------------------------------- 
GetFk_Edm_Lot 
-------------------------------------------------------------------------- */
PROCEDURE GetFk_Edm_Lot( 
   nin_           IN            Ans_Flat_Owner.Tmr_Tamro.Nordic_Item_Number%TYPE,
   pin_           IN            Ans_Flat_Owner.Tmr_Tamro.Principal_Item_Number%TYPE,
   r_Lot          IN OUT NOCOPY Edm_Lot%ROWTYPE, 
   list_fk_tab_   OUT    NOCOPY List_Tables )
IS
   xrf_fnshd_prod_id_   NUMBER;
   xrf_sts_             BINARY_INTEGER;
   i                    PLS_INTEGER := 0;
         
BEGIN
   MODULE_NM_      := 'TMR_GFEL';
   list_fk_tab_    := List_Tables();

   r_Lot.Edm_Fnshd_Prod_Id  := NULL;
         
   Exists_Xref( 'CONCORDE', TRIM(TO_CHAR(nin_, '099999')), 'EDM_FNSHD_PROD', xrf_fnshd_prod_id_, xrf_sts_ );
   IF xrf_sts_ = 0 THEN -- Not found
   
      SELECT Edm_Fnshd_Prod_Id INTO xrf_fnshd_prod_id_
      FROM Edm_Fnshd_Prod WHERE Gdms_Cd LIKE (pin_ || '%');
      
   END IF;
   
   r_Lot.Edm_Fnshd_Prod_Id := xrf_fnshd_prod_id_;   

EXCEPTION
   WHEN OTHERS THEN 
      i := i + 1;
      list_fk_tab_.EXTEND;
      list_fk_tab_(i)  := 'EDM_FNSHD_PROD';
      RETURN;
      
END GetFk_Edm_Lot;


/*-------------------------------------------------------------------------- 
GetFk_Edm_AcqrdSalesData
--------------------------------------------------------------------------*/   
PROCEDURE GetFk_Edm_AcqrdSalesData(
   r              IN OUT NOCOPY Edm_Acqrd_Sales_Data%ROWTYPE,
   customer_      IN Ans_Sad_Owner.Tmr_Street_Sales.Customer_Number_1%TYPE, 
   currency_      IN Ans_Sad_Owner.Tmr_Street_Sales.Currency%TYPE,
   nin_           IN Ans_Sad_Owner.Tmr_Street_Sales.Nordic_Item_Number%TYPE,
   brick_         IN Ans_Sad_Owner.Tmr_Street_Sales.Subgeoreg_1%TYPE,
   period_        IN NUMBER,
   list_fk_tab_   OUT    NOCOPY List_Tables,
   sk_            OUT    NOCOPY List_Keys )
IS
   microbrick_id_         NUMBER;
   xrf_sts_               BINARY_INTEGER;
   dummy_                 VARCHAR2(1);
   i                      PLS_INTEGER := 0;
   
BEGIN
   MODULE_NM_  := 'TMR_GFEASD';
   list_fk_tab_    := List_Tables();
   sk_             := List_Keys();
   
   r.Edm_Gpltcl_Area_Id    := NULL;
   r.Edm_Prd_Id            := NULL;
   r.Edm_Crncy_Id          := NULL;
   r.Edm_Mrkt_Prod_Grp_Id  := NULL;
   r.Edm_Org_Id            := NULL;
   
   -- Get key to buyer organization 
   Exists_Xref( SRC_SYS_, TO_CHAR(customer_), 'EDM_ORG', r.Edm_Org_Id, xrf_sts_ );
   IF xrf_sts_ = 0 THEN -- Not found
      i := i + 1;
      list_fk_tab_.EXTEND;
      sk_.EXTEND;
      list_fk_tab_(i)  := 'EDM_ORG';   
      sk_(i)           := TO_CHAR(customer_);
   ELSE
      BEGIN 
         SELECT Org_Typ_Cd INTO r.Org_Typ_Cd
         FROM Edm_Org
         WHERE Edm_Org_Id = r.Edm_Org_Id;
      EXCEPTION
         WHEN OTHERS THEN r.Org_Typ_Cd := 'IP'; 
      END;
   END IF;
   
   -- Get key to Market Prod Group    
   -- Exists_Xref( SRC_SYS_, TO_CHAR(rec_Ssales.Nordic_Item_Number), 'EDM_MRKT_PROD_GRP', r.Edm_Mrkt_Prod_Grp_Id, xrf_sts_ );
   Exists_Xref( 'CONCORDE', TRIM(TO_CHAR(nin_, '099999')), 'EDM_MRKT_PROD_GRP', r.Edm_Mrkt_Prod_Grp_Id, xrf_sts_ );
   IF xrf_sts_ = 0 THEN -- Not found
      i := i + 1;
      list_fk_tab_.EXTEND;
      sk_.EXTEND;
      list_fk_tab_(i)  := 'EDM_MRKT_PROD_GRP';
      sk_(i)           := TRIM(TO_CHAR(nin_, '099999'));  
   END IF;

   -- Set Period ID
   r.Edm_Prd_Id := period_;
      
   -- Get key to Geopolitical Area (V15: Optional)
   BEGIN
   
      SELECT Edm_Gpltcl_Area_Id INTO microbrick_id_ 
      FROM Edm_Gpltcl_Area G 
      WHERE G.BRICK_NBR = LPAD(brick_, 3, '0') -- 6.1
      AND ROWNUM = 1;
	  
      r.Edm_Gpltcl_Area_Id := microbrick_id_;
	     
   EXCEPTION
      WHEN OTHERS THEN 
	     -- Use default value suggested by Niclas (mail 10SEP04)
         SELECT Edm_Gpltcl_Area_Id INTO microbrick_id_ 
         FROM Edm_Gpltcl_Area G 
         WHERE G.BRICK_NBR = '099' AND ROWNUM = 1;
		 
	     r.Edm_Gpltcl_Area_Id := microbrick_id_;
   END;
   

   BEGIN
   
      -- Get key to Currency (V15: Optional) 
      SELECT Edm_Crncy_Id INTO r.Edm_Crncy_Id
      FROM Edm_Crncy
      WHERE Crncy_Cd = currency_
      AND ROWNUM = 1;

   EXCEPTION
      WHEN OTHERS THEN
	     -- V15: Assume SEK as default currency
         SELECT Edm_Crncy_Id INTO r.Edm_Crncy_Id FROM Edm_Crncy 
		 WHERE Crncy_Cd = 'SEK' AND ROWNUM = 1;   
   END;
   
         
END GetFk_Edm_AcqrdSalesData;


/*-------------------------------------------------------------------------- 
GetFk_Edm_Cust_Ordr 
--------------------------------------------------------------------------*/   
PROCEDURE GetFk_Edm_Cust_Ordr( 
   r             IN OUT NOCOPY Edm_Cust_Ordr%ROWTYPE, 
   list_fk_tab_  OUT    NOCOPY List_Tables,
   sk_           OUT    NOCOPY List_Keys )

IS
   step_    CHAR(1);
   i        PLS_INTEGER := 0;
   
BEGIN
   MODULE_NM_  := 'TMR_GFECO';
   list_fk_tab_    := List_Tables();
   sk_             := List_Keys();
 
   r.Edm_Org_Id_Buyer  := NULL;
   r.Edm_Crncy_Id      := NULL;
     
   -- fill the necessary FKs in the Edm_Xpto record
   -- return a comma-separated list of missing FKs /SKs and corresponding tables.

   step_ := '1';
   
   -- Get key to buyer organization (Tamro AB)
   SELECT Edm_Org_Id INTO r.Edm_Org_Id_Buyer
   FROM Edm_Org E
   WHERE E.Bsns_Nm = TAMRO_SWEDEN_NAME_
   AND ROWNUM = 1;
   
   step_   := '2';
   
   -- Get key to Currency   
   SELECT Edm_Crncy_Id INTO r.Edm_Crncy_Id
   FROM Edm_Crncy
   WHERE Crncy_Cd = rec_Csales.Currency
   AND ROWNUM = 1;
   
EXCEPTION
   WHEN OTHERS THEN
      IF step_ = '1' THEN
	     i := i + 1;
         list_fk_tab_.EXTEND;
         sk_.EXTEND; 
         list_fk_tab_(i)   := 'EDM_ORG';
         sk_(i)            := TAMRO_SWEDEN_NAME_;
      ELSIF step_ = '2' THEN
	     -- Assume SEK as default currency
         SELECT Edm_Crncy_Id INTO r.Edm_Crncy_Id FROM Edm_Crncy  
		 WHERE Crncy_Cd = 'SEK' AND ROWNUM = 1;
      END IF;
      RETURN;
         
END GetFk_Edm_Cust_Ordr;



/*-------------------------------------------------------------------------- 
GetFk_Edm_Outgng_Shpmnt 
--------------------------------------------------------------------------*/   
PROCEDURE GetFk_Edm_Outgng_Shpmnt( 
   r              IN OUT NOCOPY Edm_Outgng_Shpmnt%ROWTYPE, 
   list_fk_tab_       OUT    NOCOPY List_Tables,
   sk_            OUT    NOCOPY List_Keys )

IS
   step_     CHAR(1);
   xrf_sts_  BINARY_INTEGER;
   i         PLS_INTEGER := 0;
      
BEGIN
   MODULE_NM_  := 'TMR_GFEOS';
   list_fk_tab_    := List_Tables();
   sk_             := List_Keys();
   
   r.Edm_Fclty_Id               := NULL;
   r.Edm_Org_Id_To              := NULL;
   r.Edm_Buyer_Physcl_Addr_Id   := NULL;

   step_   := '1';
   -- Get key to buyer organization (Tamro AB) and Address
   SELECT Edm_Org_Id INTO r.Edm_Org_Id_To
   FROM Edm_Org E
   WHERE E.Bsns_Nm = TAMRO_SWEDEN_NAME_
   AND ROWNUM = 1;
   
   step_   := '2';   
   SELECT Edm_Org_Physcl_Adrs_Id
   INTO r.Edm_Buyer_Physcl_Addr_Id
   FROM Edm_Physcl_Adrs_Org
   WHERE Edm_Org_Id = r.Edm_Org_Id_To
   AND ROWNUM = 1;

   -- Get key to EDM_FCLTY
   r.Edm_Fclty_Id := SWE_EDM_FCLTY_ID_;

   
EXCEPTION
   WHEN OTHERS THEN 
      i := i + 1;
      list_fk_tab_.EXTEND;
      sk_.EXTEND;
      IF step_ = '1' THEN 
         list_fk_tab_(i)   := 'EDM_ORG';
         sk_(i)            := TAMRO_SWEDEN_NAME_;
      ELSIF step_ = '2' THEN      
         list_fk_tab_(i)   := 'EDM_PHYSCL_ADRS_ORG';
         sk_(i)            := TAMRO_SWEDEN_NAME_;
      END IF;
      RETURN;
         
END GetFk_Edm_Outgng_Shpmnt;
            

/*-------------------------------------------------------------------------- 
GetFk_Edm_Cust_Ordr_Line 
--------------------------------------------------------------------------*/   
PROCEDURE GetFk_Edm_Cust_Ordr_Line( 
   r             IN OUT NOCOPY Edm_Cust_Ordr_Line%ROWTYPE, 
   parent_id_    IN            Edm_Cust_Ordr.Edm_Cust_Ordr_Id%TYPE,
   list_fk_tab_  OUT    NOCOPY List_Tables,
   sk_           OUT    NOCOPY List_Keys )

IS
   step_               CHAR(1);
   edm_org_            NUMBER;
   xrf_sts_            BINARY_INTEGER;
   xrf_fnshd_prod_id_  NUMBER;
   i                   PLS_INTEGER := 0;
   
BEGIN
   MODULE_NM_  := 'TMR_GFECOL';
   list_fk_tab_    := List_Tables();
   sk_             := List_Keys();

   r.Edm_Cust_Ordr_Id                := NULL;
   r.Edm_Buyer_Physcl_Addshipped_To  := NULL;
   r.Edm_Fnshd_Prod_Id               := NULL;
  
   -- fill the necessary FKs in the Edm_Cust_Ordr_Line record
   -- return a comma-separated list of missing FKs /SKs and corresponding tables.
   IF parent_id_ IS NULL THEN 
      i := i + 1;
      list_fk_tab_.EXTEND;
      sk_.EXTEND;
      list_fk_tab_(i)   := 'EDM_CUST_ORDR';
      sk_(i)            := NULL;
      RETURN;
   ELSE
      r.Edm_Cust_Ordr_Id   := parent_id_;
   END IF;
   
   step_   := '1';
   
   -- Get key to buyer organization (Tamro AB) and Address
   SELECT Edm_Org_Id INTO edm_org_
   FROM Edm_Org E
   WHERE E.Bsns_Nm = TAMRO_SWEDEN_NAME_
   AND ROWNUM = 1;
   
   step_   := '2';   
   
   SELECT Edm_Org_Physcl_Adrs_Id
   INTO r.Edm_Buyer_Physcl_Addshipped_To
   FROM Edm_Physcl_Adrs_Org
   WHERE Edm_Org_Id = edm_org_
   AND ROWNUM = 1;

   Exists_Xref( 'CONCORDE', TRIM(TO_CHAR(rec_Csales.Nordic_Item_Number, '099999')), 'EDM_FNSHD_PROD', xrf_fnshd_prod_id_, xrf_sts_ );
   IF xrf_sts_ = 0 THEN -- Not found
   
      step_ := '3';
      
      SELECT Edm_Fnshd_Prod_Id INTO xrf_fnshd_prod_id_
      FROM Edm_Fnshd_Prod WHERE Gdms_Cd LIKE (rec_Csales.Principal_Item_Number || '%');
      
   END IF;
   
   r.Edm_Fnshd_Prod_Id := xrf_fnshd_prod_id_;   
      
EXCEPTION
   WHEN OTHERS THEN
      i := i + 1; 
      list_fk_tab_.EXTEND;
      sk_.EXTEND;
      IF step_ = '1' THEN 
         list_fk_tab_(i)   := 'EDM_ORG';
         sk_(i)            := TAMRO_SWEDEN_NAME_;
      ELSIF step_ = '2' THEN
         list_fk_tab_(i)   := 'EDM_PHYSCL_ADRS_ORG';
         sk_(i)            := TAMRO_SWEDEN_NAME_;
      ELSE
         list_fk_tab_(i)   := 'EDM_FNSHD_PROD';    
         sk_(i)            := TRIM(TO_CHAR(rec_Csales.Nordic_Item_Number, '099999'));     
      END IF;
      RETURN;
         
END GetFk_Edm_Cust_Ordr_Line;
        


/*-------------------------------------------------------------------------- 
GetFk_Edm_Outgng_Shpmnt_Line 
--------------------------------------------------------------------------*/   
PROCEDURE GetFk_Edm_Outgng_Shpmnt_Line( 
   cust_ordline_id_   IN            Edm_Cust_Ordr_Line.Edm_Cust_Ordr_Line_Id%TYPE,
   r                  IN OUT NOCOPY Edm_Outgng_Shpmnt_Line%ROWTYPE, 
   parent_id_         IN            NUMBER,
   list_fk_tab_       OUT    NOCOPY List_Tables,
   sk_                OUT    NOCOPY List_Keys )

IS
   xrf_sts_             BINARY_INTEGER;
   xrf_fnshd_prod_id_   NUMBER;
   step_                CHAR(1);
   xrf_lot_id_          NUMBER;
   xrf_fclty_id_        NUMBER;
   invntry_lot_prtn_id_ NUMBER;
   invntry_non_lot_id_  NUMBER;
   
   i                    PLS_INTEGER := 0;
         
BEGIN
   MODULE_NM_  := 'TMR_GFEOSL';
   list_fk_tab_    := List_Tables();
   sk_             := List_Keys();

   r.Edm_Outgng_Shpmnt_Id     := NULL;
   r.Edm_Fnshd_Prod_Id        := NULL;
   r.Edm_Lot_Id               := NULL;
   r.Edm_Invntry_Lot_Prtn_Id  := NULL;
   r.Edm_Invntry_Non_Lot_Id   := NULL; 
   r.Edm_Cust_Ordr_Line_Id    := NULL;
   
   -- Get Edm_Cust_Ordr_Line_Id
   IF cust_ordline_id_ IS NULL THEN 
      i := i + 1;
      list_fk_tab_.EXTEND; sk_.EXTEND;
      list_fk_tab_(i)   := 'EDM_CUST_ORDR_LINE';
      sk_(i)            := NULL;
   ELSE
      r.Edm_Cust_Ordr_Line_Id   := cust_ordline_id_;
   END IF;

   -- Get Finished Product ID
   Exists_Xref( 'CONCORDE', TRIM(TO_CHAR(rec_Csales.Nordic_Item_Number, '099999')), 'EDM_FNSHD_PROD', xrf_fnshd_prod_id_, xrf_sts_ );
   IF xrf_sts_ = 0 THEN -- Not found
      step_ := '1';
      SELECT Edm_Fnshd_Prod_Id INTO xrf_fnshd_prod_id_
      FROM Edm_Fnshd_Prod WHERE Gdms_Cd LIKE (rec_Csales.Principal_Item_Number || '%');
   END IF;
   r.Edm_Fnshd_Prod_Id := xrf_fnshd_prod_id_;   
  
   -- Get parent record ID
   IF parent_id_ IS NULL THEN 
      i := i + 1;
      list_fk_tab_.EXTEND;
      sk_.EXTEND;
      list_fk_tab_(i)   := 'EDM_OUTGNG_SHPMNT';
      sk_(i)            := NULL;
      RETURN;
   ELSE
      r.Edm_Outgng_Shpmnt_Id   := parent_id_;
   END IF;

      
   -- ALL FKs BELOW ARE NOT MANDATORY!
   
   -- Lot
   Exists_Xref( SRC_SYS_, rec_Csales.Lot, 'EDM_LOT', xrf_lot_id_, xrf_sts_ );
   IF xrf_sts_ = 0 THEN -- Not found
      r.Edm_Lot_Id  := NULL;
   ELSE 
      r.Edm_Lot_Id := xrf_lot_id_;
   END IF;   
    
   -- Invntry_Lot_Prtn
   xrf_fclty_id_  := SWE_EDM_FCLTY_ID_;
      
   IF xrf_lot_id_ IS NOT NULL AND xrf_fclty_id_ IS NOT NULL THEN

      step_ := '3';  
      SELECT Edm_Invntry_Lot_Prtn_Id 
      INTO invntry_lot_prtn_id_
      FROM Edm_Invntry_Lot_Prtn
      WHERE Edm_Fclty_Id = xrf_fclty_id_
      AND   Edm_Lot_Id = xrf_lot_id_
      AND ROWNUM = 1;   
      
      IF invntry_lot_prtn_id_ IS NOT NULL THEN
         r.Edm_Invntry_Lot_Prtn_Id := invntry_lot_prtn_id_;
         RETURN; -- According to spec, no need to have both LOT_PRTN and NON_LOT   
      ELSE
         r.Edm_Invntry_Lot_Prtn_Id := NULL;
      END IF;
   END IF;
   
   -- Invntry_Non_Lot
   Exists_Xref( 'CONCORDE', TRIM(TO_CHAR(rec_Csales.Nordic_Item_Number, '099999')), 'EDM_FNSHD_PROD', xrf_fnshd_prod_id_, xrf_sts_ );
   IF xrf_sts_ = 0 THEN -- Not found
 
      step_ := '4';        
      SELECT Edm_Fnshd_Prod_Id INTO xrf_fnshd_prod_id_ 
      FROM Edm_Fnshd_Prod WHERE Gdms_Cd LIKE (rec_Csales.Principal_Item_Number || '%');
      
      IF xrf_fnshd_prod_id_ IS NULL THEN 
         RETURN; -- Without Edm_Fnshd_Prod_Id, it won't be possible to get values of the 2nd FK
      END IF;
   END IF;

   step_ := '5';
   SELECT Edm_Invntry_Non_Lot_Id 
   INTO invntry_non_lot_id_
   FROM Edm_Invntry_Non_Lot
   WHERE Edm_Fclty_Id = xrf_fclty_id_
   AND   Edm_Fnshd_Prod_Id = xrf_fnshd_prod_id_
   AND   ROWNUM = 1;

   IF invntry_non_lot_id_ IS NOT NULL 
   THEN 
      r.Edm_Invntry_Non_Lot_Id := invntry_non_lot_id_;
      RETURN;
   ELSE
      r.Edm_Invntry_Non_Lot_Id := NULL;
      RETURN;
   END IF;
     
   
EXCEPTION
   WHEN OTHERS THEN    
      IF step_ = '1' THEN
         i := i + 1;
         list_fk_tab_.EXTEND; sk_.EXTEND;
         list_fk_tab_(i)  := 'EDM_FNSHD_PROD';
         sk_(i)           := TRIM(TO_CHAR(rec_Csales.Nordic_Item_Number, '099999'));
      END IF;
      RETURN;
         
END GetFk_Edm_Outgng_Shpmnt_Line;
   


/*-------------------------------------------------------------------------- 
GetFk_Edm_Invntry_Lot_Prtn
--------------------------------------------------------------------------*/   
PROCEDURE GetFk_Edm_Invntry_Lot_Prtn(
   t_qty          IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Qty%TYPE,
   t_lot          IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Lot%TYPE,
   t_warehouse    IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Warehouse%TYPE,
   r              IN OUT NOCOPY Edm_Invntry_Lot_Prtn%ROWTYPE, 
   list_fk_tab_   OUT    NOCOPY List_Tables,
   sk_            OUT    NOCOPY List_Keys  )

IS
   xrf_fclty_id_          NUMBER;
   xrf_lot_id_            NUMBER;
   xrf_sts_               BINARY_INTEGER;
   step_                  CHAR(1);
   i                      PLS_INTEGER := 0;
   curr_qty_              NUMBER;
   
BEGIN
   MODULE_NM_  := 'TMR_GFEILP';
   list_fk_tab_    := List_Tables();
   sk_             := List_Keys(); 

   r.Edm_Fclty_Id  := NULL;
   r.Edm_Lot_Id    := NULL;
   r.Curr_Qty      := NULL;
   
   -- Get EDM_LOT_ID 
   Exists_Xref( SRC_SYS_, t_lot, 'EDM_LOT', xrf_lot_id_, xrf_sts_ );
   IF xrf_sts_ = 0 THEN  -- Not found
      i := i + 1;
      list_fk_tab_.EXTEND; sk_.EXTEND;
      list_fk_tab_(i)  := 'EDM_LOT';
      sk_(i)           := t_lot;
   ELSE
      r.Edm_Lot_Id := xrf_lot_id_;
   END IF;
   
   -- Get EDM_FCLTY_ID
   xrf_fclty_id_  := SWE_EDM_FCLTY_ID_;
   r.Edm_Fclty_Id := xrf_fclty_id_;
   
   r.Curr_Qty := t_qty;
     
EXCEPTION
   WHEN OTHERS THEN    
      IF step_ = '1' THEN
         i := i + 1;
         list_fk_tab_.EXTEND; sk_.EXTEND;
         list_fk_tab_(i)  := 'EDM_FCLTY';
         sk_(i)           := t_warehouse;
      END IF;
	  IF step_ = '2' THEN 
         i := i + 1;
         list_fk_tab_.EXTEND; sk_.EXTEND;
         list_fk_tab_(i)  := 'EDM_INVNTRY_LOT_PRTN';
         sk_(i)           := t_warehouse || ',' || t_lot;
	  END IF;
      RETURN;
   
END GetFk_Edm_Invntry_Lot_Prtn;



/*-------------------------------------------------------------------------- 
GetFk_Edm_Invntry_Non_Lot
--------------------------------------------------------------------------*/   
PROCEDURE GetFk_Edm_Invntry_Non_Lot(
   t_qty          IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Qty%TYPE,  
   t_nin          IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Nordic_Item_Number%TYPE,
   t_pin          IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Principal_Item_Number%TYPE,
   t_warehouse    IN            Ans_Sad_Owner.Tmr_Consignment_Stock.Warehouse%TYPE,
   r              IN OUT NOCOPY Edm_Invntry_Non_Lot%ROWTYPE, 
   list_fk_tab_   OUT    NOCOPY List_Tables,
   sk_            OUT    NOCOPY List_Keys  )
IS
   xrf_fclty_id_          NUMBER;
   xrf_fnshd_prod_id_     NUMBER;
   xrf_sts_               BINARY_INTEGER;
   step_                  CHAR(1);
   i                      PLS_INTEGER := 0;
   curr_qty_              NUMBER;
   
BEGIN
   MODULE_NM_  := 'TMR_GFEINL';
   list_fk_tab_    := List_Tables();
   sk_             := List_Keys(); 

   r.Edm_Fclty_Id       := NULL;
   r.Edm_Fnshd_Prod_Id  := NULL;
   r.Curr_Qty           := NULL;
   
   -- Get EDM_FCLTY_ID
   xrf_fclty_id_ := SWE_EDM_FCLTY_ID_;
   r.Edm_Fclty_Id := xrf_fclty_id_;

   -- Get EDM_FNSHD_PROD_ID
   Exists_Xref( 'CONCORDE', TRIM(TO_CHAR(t_nin, '099999')), 'EDM_FNSHD_PROD', xrf_fnshd_prod_id_, xrf_sts_ );
   IF xrf_sts_ = 0 THEN -- Not found
      step_ := '2';
      SELECT Edm_Fnshd_Prod_Id INTO xrf_fnshd_prod_id_ 
      FROM Edm_Fnshd_Prod WHERE Gdms_Cd LIKE (t_pin || '%');
   END IF;
   
   r.Edm_Fnshd_Prod_Id := xrf_fnshd_prod_id_;

 
   -- Get Current Qty and add the qty movement
   BEGIN
      step_ := '3';
      SELECT Curr_Qty INTO curr_qty_
      FROM Edm_Invntry_Non_Lot A
      WHERE A.Edm_Fnshd_Prod_Id = r.Edm_Fnshd_Prod_Id AND A.Edm_Fclty_Id = r.Edm_Fclty_Id
      AND A.Invntry_As_At_Dt = (SELECT MAX(B.Invntry_As_At_Dt) FROM Edm_Invntry_Non_Lot B WHERE B.Edm_Fnshd_Prod_Id = r.Edm_Fnshd_Prod_Id AND B.Edm_Fclty_Id = r.Edm_Fclty_Id)
      AND ROWNUM = 1;
   EXCEPTION
      WHEN NO_DATA_FOUND THEN -- No initial stock value for this product => Assume Zero
         Event_Log.log_event(TRIM(TO_CHAR(t_nin, '099999')) || ' :Initial stock value assumed 0', 'TMR_GFEINL', 'W');
         curr_qty_ := 0;
		 GOTO update_qty;
   END;

	 
<<update_qty>>
   -- Event_Log.log_event('DBG> Prod, OldLevel ' || r.Edm_Fnshd_Prod_Id  || ',' || curr_qty_, 'TMR_GFEINL', 'I');
   r.Curr_Qty := curr_qty_ + t_qty;
   -- Event_Log.log_event('DBG> Prod, TransQty, NewLevel ' || r.Edm_Fnshd_Prod_Id  || ',' || t_qty || ',' || r.Curr_Qty  , 'TMR_GFEINL', 'I');
   
EXCEPTION
   WHEN OTHERS THEN    
      IF step_ = '1' THEN
         i := i + 1;
         list_fk_tab_.EXTEND; sk_.EXTEND;
         list_fk_tab_(i)  := 'EDM_FCLTY';
         sk_(i)           := t_warehouse;
      END IF;
      IF step_ = '2' THEN
         i := i + 1;
         list_fk_tab_.EXTEND; sk_.EXTEND;
         list_fk_tab_(i)  := 'EDM_FNSHD_PROD';
         sk_(i)           := TRIM(TO_CHAR(t_nin, '099999'));
      END IF;   
      IF step_ = '3' THEN
         i := i + 1;
         list_fk_tab_.EXTEND; sk_.EXTEND;
         list_fk_tab_(i)  := 'EDM_INVNTRY_NON_LOT';
         sk_(i)           := TO_CHAR(SWE_EDM_FCLTY_ID_) || ',' || TRIM(TO_CHAR(t_nin, '099999'));
      END IF;     
      RETURN;
   
END GetFk_Edm_Invntry_Non_Lot;


/* -------------------------------------------------------------------------- 
Insert_Edm_Item_Prc
-------------------------------------------------------------------------- */   
PROCEDURE Insert_Edm_Item_Prc  (
   r             IN OUT NOCOPY Edm_Item_Prc%ROWTYPE,
   result_       OUT BINARY_INTEGER ) 
IS 
BEGIN

   MODULE_NM_  := 'TMR_IEIP';
   result_ := 0;
   
   INSERT INTO Edm_Item_Prc 
   (
      Edm_Item_Prc_Id,
      Prc_Typ_Cd,
      Edm_Crncy_Id,
      Edm_Gpltcl_Area_Id,
      Edm_Fnshd_Prod_Id,
      Efctv_Dt,
      Unt_Prc,
      Prc_Sts,
      Org_Typ_Cd,
      Edm_Bsns_Grp_Id,
      Edm_Prmry_Src_Systm_Cd,
      Edm_Batch_Ld_Id
   )
   VALUES
   (
      Ans_Edm_Owner.Edm_Item_Prc_Seq.NEXTVAL,
      r.Prc_Typ_Cd,
      r.Edm_Crncy_Id,
      r.Edm_Gpltcl_Area_Id,
      r.Edm_Fnshd_Prod_Id,
      r.Efctv_Dt,
      r.Unt_Prc,
      r.Prc_Sts,
      r.Org_Typ_Cd,
      r.Edm_Bsns_Grp_Id,
      r.Edm_Prmry_Src_Systm_Cd,
      r.Edm_Batch_Ld_Id     
   );
   
EXCEPTION
   WHEN DUP_VAL_ON_INDEX THEN 
   	   -- There's already a price for the same product & day!
   	   -- Update existing Line instead of Inserting! sts='A', unt_prc=r.Unt_Prc  
	     UPDATE Ans_Edm_Owner.Edm_Item_Prc 
	     SET    Prc_Sts = 'A', Unt_Prc = r.Unt_Prc   
	     WHERE  Edm_Fnshd_Prod_Id = r.Edm_Fnshd_Prod_Id 
		    AND Efctv_Dt = r.Efctv_Dt;	   
	   NULL;
   
   WHEN OTHERS THEN
      result_ := -1;
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
 
END Insert_Edm_Item_Prc;


/* -------------------------------------------------------------------------- 
Insert_Edm_Lot
-------------------------------------------------------------------------- */
PROCEDURE Insert_Edm_Lot (
   r_Lot                   IN OUT NOCOPY Edm_Lot%ROWTYPE, 
   result_                 OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_   := 'TMR_IEL';
   result_ := 0;
   
   INSERT INTO Edm_Lot
   (
     Edm_Lot_Id ,
     Lot_Id_Cd ,
     -- Batch_Qty    
     Edm_Fnshd_Prod_Id  ,
     Prchsd_Flg      ,
     -- Schdld_Rls_Dt  
     -- Site_Prdcd_Cd             
     -- Lot_Sts                  
     -- Lot_Crt_Dt              
     Lot_Expry_Dt     ,
     -- Orgnl_Qty                    
     -- Anlys_Crtfctn_Cd             
     -- Aprvl_Dt                     
     -- Lot_Ptncy                   
     -- Mftr_Dt                      
     -- Mftr_Lot_Nbr                 
     -- Lot_Rcvd_Dt                
     -- Lot_Rtst_Dt                
     -- Qa_Aprvl_Dt                 
     -- Edm_Prmry_Src_Systm_Del_Flg  
     Edm_Prmry_Src_Systm_Cd, 
     -- Edm_Ld_Trnsctn_Usr          
     -- Edm_Ld_Trnsctn_Dt            
     -- LD_TRNSCTN_TYP_CD      
     Edm_Batch_Ld_Id           
     -- Edm_Rpt_Rnge_Efctv_Dt    
     -- Edm_Rpt_Rnge_Exprtn_Dt      
     -- Xrf_Id                 
   )
   VALUES
   (
     r_Lot.Edm_Lot_Id,
     r_Lot.Lot_Id_Cd,
     -- BATCH_QTY    
     r_Lot.Edm_Fnshd_Prod_Id,
     0,
     -- SCHDLD_RLS_DT 
     -- SITE_PRDCD_CD    
     -- LOT_STS                  
     -- LOT_CRT_DT          
     r_Lot.Lot_Expry_Dt,
     -- ORGNL_QTY                    
     -- ANLYS_CRTFCTN_CD             
     -- APRVL_DT                     
     -- LOT_PTNCY                   
     -- MFTR_DT                      
     -- MFTR_LOT_NBR                 
     -- LOT_RCVD_DT                
     -- LOT_RTST_DT                
     -- QA_APRVL_DT                 
     -- EDM_PRMRY_SRC_SYSTM_DEL_FLG  
     SRC_SYS_,
     -- EDM_LD_TRNSCTN_USR          
     -- EDM_LD_TRNSCTN_DT            
     -- LD_TRNSCTN_TYP_CD      
     LD_ID_          
     -- EDM_RPT_RNGE_EFCTV_DT    
     -- EDM_RPT_RNGE_EXPRTN_DT   
     -- XRF_ID     
   );

EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Edm_Lot;



/*-------------------------------------------------------------------------- 
Insert_Ext_Invntry_Mvmnt
--------------------------------------------------------------------------*/
PROCEDURE Insert_Ext_Invntry_Mvmnt (
   r                    IN OUT NOCOPY Ans_Core_Ext_Owner.Ext_Invntry_Mvmnt%ROWTYPE, 
   result_              OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_   := 'TMR_IEIM';
   result_ := 0;
   
   INSERT INTO Ans_Core_Ext_Owner.Ext_Invntry_Mvmnt
   (
     Edm_Invntry_Mvmnt_Id     ,
     Edm_Invntry_Non_Lot_Id   ,
     Edm_Invntry_Lot_Prtn_Id  ,
     Invntry_Mvmnt_Qty        ,
     Invntry_Mvmnt_Dt         ,
     Invntry_Mvmnt_Amt        ,
     -- Invntry_Mvmnt_Typ     ,
     Invntry_Mvmnt_Typ_Cd     , 
     Mvmnt_Reason_Cd         
     -- Doc_Nbr                 
     -- Expry_Dt             
   )
   VALUES
   (
     r.Edm_Invntry_Mvmnt_Id,
     r.Edm_Invntry_Non_Lot_Id,
     r.Edm_Invntry_Lot_Prtn_Id,
     r.Invntry_Mvmnt_Qty,
     r.Invntry_Mvmnt_Dt         ,
     r.Invntry_Mvmnt_Amt        ,
     -- r.Invntry_Mvmnt_Typ     ,
     r.Invntry_Mvmnt_Typ_Cd     , 
     r.Mvmnt_Reason_Cd    
     -- Doc_Nbr                 
     -- Expry_Dt       

   );

   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Ext_Invntry_Mvmnt;



/*-------------------------------------------------------------------------- 
Insert_Edm_AcqrdSalesData
--------------------------------------------------------------------------*/
PROCEDURE Insert_Edm_AcqrdSalesData (
   r                    IN OUT NOCOPY    Edm_Acqrd_Sales_Data%ROWTYPE, 
   result_              OUT              BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_   := 'TMR_IEASD';
   result_ := 0;
   
   INSERT INTO Edm_Acqrd_Sales_Data
   (
      Edm_Acqrd_Sales_Data_Id,
      Uom_Cd                       ,
      Acqrd_Sales_Data_Typ_Cd      ,
      Sold_Qty                     ,
      Lcl_Crncy_Fincl_Val          ,
      Euro_Fincl_Val               ,
      -- Prl_Imprt_Flg                ,
      Edm_Gpltcl_Area_Id           ,
      Edm_Prd_Id                             ,
      -- Edm_Mrkt_Sgmnt_Id            ,
      Edm_Crncy_Id                 ,
      Edm_Org_Id_Prvd_By           ,
      Edm_Mrkt_Prod_Grp_Id         ,
      Edm_Org_Id                   ,
      Org_Typ_Cd ,
      -- Edm_Prmry_Src_Systm_Del_Flg  ,
      Edm_Prmry_Src_Systm_Cd ,
      -- Edm_Ld_Trnsctn_Usr           ,
      -- Edm_Ld_Trnsctn_Dt            ,
      -- LD_TRNSCTN_TYP_CD        ,
     Edm_Batch_Ld_Id
     -- Edm_Rpt_Rnge_Efctv_Dt        ,
     -- Edm_Rpt_Rnge_Exprtn_Dt       ,
     -- Xrf_Id                        
   )
   VALUES
   (
      r.Edm_Acqrd_Sales_Data_Id,
      r.Uom_Cd                       ,
      r.Acqrd_Sales_Data_Typ_Cd      ,
      r.Sold_Qty                     ,
      r.Lcl_Crncy_Fincl_Val          ,
      r.Euro_Fincl_Val               ,
      -- Prl_Imprt_Flg                ,
      r.Edm_Gpltcl_Area_Id           ,
      r.Edm_Prd_Id                             ,
      -- Edm_Mrkt_Sgmnt_Id            ,
      r.Edm_Crncy_Id                 ,
      r.Edm_Org_Id_Prvd_By           ,
      r.Edm_Mrkt_Prod_Grp_Id         ,
      r.Edm_Org_Id                   ,
      r.Org_Typ_Cd ,
      -- Edm_Prmry_Src_Systm_Del_Flg  ,
      SRC_SYS_,
      -- Edm_Ld_Trnsctn_Usr           ,
      -- Edm_Ld_Trnsctn_Dt            ,
      -- LD_TRNSCTN_TYP_CD        ,
      LD_ID_
      -- Edm_Rpt_Rnge_Efctv_Dt        ,
      -- Edm_Rpt_Rnge_Exprtn_Dt       ,
      -- Xrf_Id              
   );
   
EXCEPTION
   WHEN DUP_VAL_ON_INDEX THEN 
      -- Transaction exists: update fact values 
      UPDATE Edm_Acqrd_Sales_Data 
      SET 
         Sold_Qty            =  Sold_Qty + r.Sold_Qty,                  
         Lcl_Crncy_Fincl_Val =  Lcl_Crncy_Fincl_Val + r.Lcl_Crncy_Fincl_Val,
         Euro_Fincl_Val      =  Euro_Fincl_Val + r.Euro_Fincl_Val
      WHERE
         Edm_Prd_Id            = r.Edm_Prd_Id AND
         Edm_Gpltcl_Area_Id    = r.Edm_Gpltcl_Area_Id AND
         Org_Typ_Cd            = r.Org_Typ_Cd AND
         Edm_Org_Id            = r.Edm_Org_Id AND
         Edm_Crncy_Id          = r.Edm_Crncy_Id AND
         Edm_Mrkt_Prod_Grp_Id  = r.Edm_Mrkt_Prod_Grp_Id;
         -- Event_Log.log_event('Updated row DT-ORG-PROD:' || r.Edm_Prd_Id || '-' || r.Edm_Org_Id || '-' || r.Edm_Mrkt_Prod_Grp_Id, MODULE_NM_, 'I');
      result_ := 0;
      RETURN;
	  
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Edm_AcqrdSalesData;



/*-------------------------------------------------------------------------- 
Insert_Edm_Cust_Ordr 
--------------------------------------------------------------------------*/
PROCEDURE Insert_Edm_Cust_Ordr (
   r                    IN OUT NOCOPY    Edm_Cust_Ordr%ROWTYPE, 
   result_              OUT              BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_   := 'TMR_IECO';
   result_ := 0;

   INSERT INTO Edm_Cust_Ordr 
   (
      Edm_Cust_Ordr_Id,
      Edm_Org_Id_Buyer ,
      Edm_Crncy_Id     ,
      Cstmr_Ordr_Nbr   ,
      Ordr_Dt,
      Edm_Prmry_Src_Systm_Cd,
      Edm_Batch_Ld_Id
   )
   VALUES
   (
      r.Edm_Cust_Ordr_Id, 
      r.Edm_Org_Id_Buyer ,
      r.Edm_Crncy_Id     ,
      r.Cstmr_Ordr_Nbr   ,
      r.Ordr_Dt,
      SRC_SYS_,
      LD_ID_
   );
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Edm_Cust_Ordr;


/*-------------------------------------------------------------------------- 
Insert_Edm_Outgng_Shpmnt 
--------------------------------------------------------------------------*/
PROCEDURE Insert_Edm_Outgng_Shpmnt (
   r                    IN OUT NOCOPY    Edm_Outgng_Shpmnt%ROWTYPE, 
   result_              OUT              BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_   := 'TMR_IEOS';
   result_ := 0;
   
   INSERT INTO Edm_Outgng_Shpmnt 
   (
      Edm_Outgng_Shpmnt_Id,
      Edm_Fclty_Id,             
      Edm_Buyer_Physcl_Addr_Id,    
      Edm_Org_Id_Shpr,           
      Edm_Org_Id_To,          
      Outgng_Shpmnt_Id,      
      Exprtr_Ein_Nbr,                     
      Edm_Prmry_Src_Systm_Cd,
      Edm_Batch_Ld_Id       
   )
   VALUES
   (
      r.Edm_Outgng_Shpmnt_Id, 
      r.Edm_Fclty_Id,             
      r.Edm_Buyer_Physcl_Addr_Id,    
      r.Edm_Org_Id_Shpr,           
      r.Edm_Org_Id_To,          
      r.Outgng_Shpmnt_Id,      
      r.Exprtr_Ein_Nbr,                     
      SRC_SYS_,
      LD_ID_    
   );
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Edm_Outgng_Shpmnt;



/*-------------------------------------------------------------------------- 
Insert_Edm_Cust_Ordr_Line 
--------------------------------------------------------------------------*/
PROCEDURE Insert_Edm_Cust_Ordr_Line (
   r                    IN OUT NOCOPY    Edm_Cust_Ordr_Line%ROWTYPE, 
   result_              OUT              BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_   := 'TMR_IECOL';
   result_ := 0;
   
   INSERT INTO Edm_Cust_Ordr_Line 
   (
      Edm_Cust_Ordr_Line_Id,
      Edm_Buyer_Physcl_Addshipped_To ,
      Edm_Cust_Ordr_Id          ,
      Cstmr_Ordr_Line_Nbr  ,
      Uom_Cd   ,
      Ordr_Qty   ,
  	  Lst_Prc_Amt,
      Net_Slng_Amt,
	  Net_Slng_Amt_Euro, 
      Edm_Fnshd_Prod_Id ,
      Edm_Prmry_Src_Systm_Cd  ,
      Edm_Batch_Ld_Id
   )
   VALUES
   (
      r.Edm_Cust_Ordr_Line_Id, 
      r.Edm_Buyer_Physcl_Addshipped_To ,
      r.Edm_Cust_Ordr_Id          ,
      r.Cstmr_Ordr_Line_Nbr  ,
      r.Uom_Cd   ,
      r.Ordr_Qty   ,
  	  r.Lst_Prc_Amt,
      r.Net_Slng_Amt,
	  r.Net_Slng_Amt_Euro,  
      r.Edm_Fnshd_Prod_Id ,
      SRC_SYS_,
      LD_ID_
   );
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Edm_Cust_Ordr_Line;

              

/*-------------------------------------------------------------------------- 
Insert_Edm_Outgng_Shpmnt_Line 
--------------------------------------------------------------------------*/
PROCEDURE Insert_Edm_Outgng_Shpmnt_Line (
   r                    IN OUT NOCOPY   Edm_Outgng_Shpmnt_Line%ROWTYPE, 
   result_              OUT             BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_   := 'TMR_IEOSL';
   result_      := 0;
   
   INSERT INTO Edm_Outgng_Shpmnt_Line 
   (
      Edm_Outgng_Shpmnt_Line_Id,
      Edm_Outgng_Shpmnt_Id   ,
      Edm_Cust_Ordr_Line_Id  ,
      Edm_Fnshd_Prod_Id     ,
      Uom_Cd             ,
      Outgng_Shpmnt_Line_Nbr ,
      Edm_Lot_Id           ,
      Edm_Invntry_Non_Lot_Id   ,
      Edm_Invntry_Lot_Prtn_Id   ,
      Shp_Qty               ,
      Edm_Prmry_Src_Systm_Cd       ,
      Edm_Batch_Ld_Id
   )
   VALUES
   (
      r.Edm_Outgng_Shpmnt_Line_Id, 
      r.Edm_Outgng_Shpmnt_Id   ,
      r.Edm_Cust_Ordr_Line_Id  ,
      r.Edm_Fnshd_Prod_Id     ,
      r.Uom_Cd             ,
      r.Outgng_Shpmnt_Line_Nbr ,
      r.Edm_Lot_Id           ,
      r.Edm_Invntry_Non_Lot_Id   ,
      r.Edm_Invntry_Lot_Prtn_Id   ,
      r.Shp_Qty               ,
      SRC_SYS_,
      LD_ID_
   );
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Edm_Outgng_Shpmnt_Line;


/*-------------------------------------------------------------------------- 
Insert_Edm_Invntry_Lot_Prtn
--------------------------------------------------------------------------*/
PROCEDURE Insert_Edm_Invntry_Lot_Prtn (
   r                    IN OUT NOCOPY   Edm_Invntry_Lot_Prtn%ROWTYPE, 
   result_              OUT             BINARY_INTEGER,
   upd_pk               OUT             Edm_Invntry_Lot_Prtn.Edm_Invntry_Lot_Prtn_Id%TYPE )
IS
BEGIN
   MODULE_NM_   := 'TMR_IEILP';
   result_      := 0;
   upd_pk       := NULL;
      
   INSERT INTO Edm_Invntry_Lot_Prtn
   (
      Edm_Invntry_Lot_Prtn_Id,
      Edm_Fclty_Id, 
      Edm_Lot_Id,                
      Curr_Qty,                   
      Loc_Desc,                          
      Invntry_As_At_Dt,
      Edm_Prmry_Src_Systm_Cd,   
      Edm_Batch_Ld_Id        
  )
  VALUES
  (
      r.Edm_Invntry_Lot_Prtn_Id,
      r.Edm_Fclty_Id, 
      r.Edm_Lot_Id,                
      r.Curr_Qty,                   
      r.Loc_Desc,                             
      r.Invntry_As_At_Dt,
      SRC_SYS_,
      LD_ID_  
  );
   
EXCEPTION

   WHEN DUP_VAL_ON_INDEX THEN 
      -- 6.0: Transaction exists: update fact values 
      UPDATE Edm_Invntry_Lot_Prtn 
      SET 
         Curr_Qty    =  Curr_Qty + r.Curr_Qty                  
      WHERE
         Edm_Fclty_Id            = r.Edm_Fclty_Id AND
         Edm_Lot_Id              = r.Edm_Lot_Id AND
         Invntry_As_At_Dt        = r.Invntry_As_At_Dt AND
		 ROWNUM = 1
	  RETURNING Edm_Invntry_Lot_Prtn_Id INTO upd_pk;
		 
      Event_Log.log_event('Updated row PK: ' || upd_pk, MODULE_NM_, 'I');
      result_ := 0;
      RETURN;
	  
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Edm_Invntry_Lot_Prtn;



/*-------------------------------------------------------------------------- 
Insert_Edm_Invntry_Non_Lot
--------------------------------------------------------------------------*/
PROCEDURE Insert_Edm_Invntry_Non_Lot (
   r                    IN OUT NOCOPY   Edm_Invntry_Non_Lot%ROWTYPE, 
   result_              OUT             BINARY_INTEGER,
   upd_pk               OUT             Edm_Invntry_Non_Lot.Edm_Invntry_Non_Lot_Id%TYPE )
IS
BEGIN
   MODULE_NM_   := 'TMR_IEINL';
   result_      := 0;
   upd_pk       := NULL;

  -- Event_Log.log_event('DBG> IEINL-' || r.Edm_Fnshd_Prod_Id || '-' ||TO_CHAR(r.Invntry_As_At_Dt) || '-' || r.Curr_Qty, MODULE_NM_, 'I');
  
  INSERT INTO Edm_Invntry_Non_Lot
  (
      Edm_Invntry_Non_Lot_Id,
      Edm_Fclty_Id,
      Edm_Fnshd_Prod_Id,
      Invntry_As_At_Dt,
      Curr_Qty,
      Invntry_Sts,
      Loc_Desc,
      Edm_Prmry_Src_Systm_Cd,   
      Edm_Batch_Ld_Id        
  )
  VALUES
  (
      r.Edm_Invntry_Non_Lot_Id,
      r.Edm_Fclty_Id,
      r.Edm_Fnshd_Prod_Id,
      r.Invntry_As_At_Dt,
      r.Curr_Qty,
      r.Invntry_Sts,
      r.Loc_Desc,
      SRC_SYS_,
      LD_ID_  
  );

   
EXCEPTION
   WHEN DUP_VAL_ON_INDEX THEN 
      -- 6.0: Transaction exists: update fact values
	  -- 6.3. set Curr_Qty = Curr_Qty + r.Curr_Qty => only r.Curr_Qty 
      UPDATE Edm_Invntry_Non_Lot 
      SET 
         Curr_Qty = r.Curr_Qty                     
      WHERE
         Edm_Fclty_Id            = r.Edm_Fclty_Id AND
         Edm_Fnshd_Prod_Id       = r.Edm_Fnshd_Prod_Id AND
         Invntry_As_At_Dt        = r.Invntry_As_At_Dt AND
		 ROWNUM = 1
	  RETURNING Edm_Invntry_Non_Lot_Id INTO upd_pk;
	  		 
	  Event_Log.log_event('Updated row PK: ' || upd_pk, MODULE_NM_, 'I');
      result_ := 0;
      RETURN;
	  
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Insert_Edm_Invntry_Non_Lot;

               
/* -------------------------------------------------------------------------- 
Update_Edm_Lot
-------------------------------------------------------------------------- */
PROCEDURE Update_Edm_Lot (
   r_Lot            IN OUT NOCOPY Edm_Lot%ROWTYPE,  
   result_          OUT           BINARY_INTEGER )
IS

edm_fnshd_prod_id_    NUMBER;
edm_expiry_dt_        DATE;

BEGIN
   MODULE_NM_   := 'TMR_UEL';
   result_ := 0;
   
   -- First, verify if the UPDATE is really necessary (ie, if there is 
   -- changed data items).
   -- If it is not necessary, RETURN immediately 
   
   SELECT Edm_Fnshd_Prod_Id, Lot_Expry_Dt
   INTO edm_fnshd_prod_id_, edm_expiry_dt_
   FROM Edm_Lot
   WHERE Edm_Lot_Id = r_Lot.Edm_Lot_Id;
   
   IF r_Lot.Edm_Fnshd_Prod_Id = edm_fnshd_prod_id_ AND
      r_Lot.Lot_Expry_Dt      = edm_expiry_dt_ THEN
      RETURN;
   END IF; 
   
   UPDATE Edm_Lot
   SET Edm_Fnshd_Prod_Id = r_Lot.Edm_Fnshd_Prod_Id,
       Lot_Expry_Dt      = r_Lot.Lot_Expry_Dt
   WHERE Edm_Lot_Id = r_Lot.Edm_Lot_Id;
   
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;

END Update_Edm_Lot;
               


/* -------------------------------------------------------------------------- 
Insert_Tmr_Lot_Log (also inserts in LOG_MSG table)
-------------------------------------------------------------------------- */   
PROCEDURE Insert_Tmr_Lot_Log  (
   log_sts_                IN            VARCHAR2,
   log_msg_                IN            Log_Msg.Log_Msg%TYPE,
   r_Lot                   IN OUT NOCOPY Edm_Lot%ROWTYPE, 
   result_                 OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_ITLL';
   result_ := 0;
   
   INSERT INTO Ans_Sad_Owner.Tmr_Lot_Log
   (
     Log_Table_Id, Log_Status,
     Edm_Lot_Id ,
     Lot_Id_Cd ,
     -- Batch_Qty    
     Edm_Fnshd_Prod_Id  ,
     Prchsd_Flg      ,
     -- Schdld_Rls_Dt  
     -- Site_Prdcd_Cd             
     -- Lot_Sts                  
     -- Lot_Crt_Dt              
     Lot_Expry_Dt     ,
     -- Orgnl_Qty                    
     -- Anlys_Crtfctn_Cd             
     -- Aprvl_Dt                     
     -- Lot_Ptncy                   
     -- Mftr_Dt                      
     -- Mftr_Lot_Nbr                 
     -- Lot_Rcvd_Dt                
     -- Lot_Rtst_Dt                
     -- Qa_Aprvl_Dt                 
     -- Edm_Prmry_Src_Systm_Del_Flg  
     Edm_Prmry_Src_Systm_Cd, 
     Ld_Trnsctn_Usr,          
     Ld_Trnsctn_Dt,            
     -- LD_TRNSCTN_TYP_CD      
     Batch_Ld_Id           
     -- Edm_Rpt_Rnge_Efctv_Dt    
     -- Edm_Rpt_Rnge_Exprtn_Dt      
     -- Xrf_Id                 

   )
   VALUES
   (
     Log_Table_Seq.NEXTVAL, log_sts_,
     NULL,
     r_Lot.Lot_Id_Cd,
     -- BATCH_QTY    
     r_Lot.Edm_Fnshd_Prod_Id,
     0,
     -- SCHDLD_RLS_DT 
     -- SITE_PRDCD_CD    
     -- LOT_STS                  
     -- LOT_CRT_DT          
     r_Lot.Lot_Expry_Dt,
     -- ORGNL_QTY                    
     -- ANLYS_CRTFCTN_CD             
     -- APRVL_DT                     
     -- LOT_PTNCY                   
     -- MFTR_DT                      
     -- MFTR_LOT_NBR                 
     -- LOT_RCVD_DT                
     -- LOT_RTST_DT                
     -- QA_APRVL_DT                 
     -- EDM_PRMRY_SRC_SYSTM_DEL_FLG  
     SRC_SYS_,
     USER,          
     SYSDATE,            
     -- LD_TRNSCTN_TYP_CD      
     LD_ID_          
     -- EDM_RPT_RNGE_EFCTV_DT    
     -- EDM_RPT_RNGE_EXPRTN_DT   
     -- XRF_ID     
   );
   
   
   INSERT INTO Log_Msg
   (
      Log_Id, Log_Record_Table, Log_Source_Key, 
      Log_Type, Log_Msg, Batch_Ld_Id 
   ) 
   VALUES
   (
      Log_Msg_Seq.NEXTVAL, 'TMR_LOT_LOG', Log_Table_Seq.CURRVAL, 
      DECODE(log_sts_, 'H','H',  'E','N',  'P' ), log_msg_, LD_ID_
   );
      
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;
      
END Insert_Tmr_Lot_Log;
	  

/*-------------------------------------------------------------------------- 
Insert_Tmr_AcqrdSalesData_Log (also inserts in LOG_MSG table)
--------------------------------------------------------------------------*/   
PROCEDURE Insert_Tmr_AcqrdSalesData_Log  (
   log_sts_                IN            VARCHAR2,
   log_msg_                IN            Log_Msg.Log_Msg%TYPE,
   r                       IN OUT NOCOPY Edm_Acqrd_Sales_Data%ROWTYPE, 
   result_                 OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_ITASDL';
   result_ := 0;
   
   INSERT INTO Ans_Sad_Owner.Tmr_Acqrd_Sales_Data_Log
   (
      Log_Table_Id, Log_Status,
      Edm_Acqrd_Sales_Data_Id,
      Uom_Cd                       ,
      Acqrd_Sales_Data_Typ_Cd      ,
      Sold_Qty                     ,
      Lcl_Crncy_Fincl_Val          ,
      Euro_Fincl_Val               ,
      -- Prl_Imprt_Flg                ,
      Edm_Gpltcl_Area_Id           ,
      Edm_Prd_Id                             ,
      -- Edm_Mrkt_Sgmnt_Id            ,
      Edm_Crncy_Id                 ,
      Edm_Org_Id_Prvd_By           ,
      Edm_Mrkt_Prod_Grp_Id         ,
      Edm_Org_Id                   ,
      Org_Typ_Cd ,
      -- Edm_Prmry_Src_Systm_Del_Flg  ,
      Edm_Prmry_Src_Systm_Cd ,
      Ld_Trnsctn_Usr           ,
      Ld_Trnsctn_Dt            ,
      -- LD_TRNSCTN_TYP_CD        ,
     Batch_Ld_Id
     -- Edm_Rpt_Rnge_Efctv_Dt        ,
     -- Edm_Rpt_Rnge_Exprtn_Dt       ,
     -- Xrf_Id                        
   )
   VALUES
   (
      Log_Table_Seq.NEXTVAL, log_sts_,
      NULL,
      r.Uom_Cd                       ,
      r.Acqrd_Sales_Data_Typ_Cd      ,
      r.Sold_Qty                     ,
      r.Lcl_Crncy_Fincl_Val          ,
      r.Euro_Fincl_Val               ,
      -- Prl_Imprt_Flg                ,
      r.Edm_Gpltcl_Area_Id           ,
      r.Edm_Prd_Id                             ,
      -- Edm_Mrkt_Sgmnt_Id            ,
      r.Edm_Crncy_Id                 ,
      r.Edm_Org_Id_Prvd_By           ,
      r.Edm_Mrkt_Prod_Grp_Id         ,
      r.Edm_Org_Id                   ,
      r.Org_Typ_Cd ,
      -- Edm_Prmry_Src_Systm_Del_Flg  ,
      SRC_SYS_,
      USER           ,
      SYSDATE            ,
      -- LD_TRNSCTN_TYP_CD        ,
      LD_ID_
      -- Edm_Rpt_Rnge_Efctv_Dt        ,
      -- Edm_Rpt_Rnge_Exprtn_Dt       ,
      -- Xrf_Id            
   );
   
   
   INSERT INTO Log_Msg
   (
      Log_Id, Log_Record_Table, Log_Source_Key, 
      Log_Type, Log_Msg, Batch_Ld_Id 
   ) 
   VALUES
   (
      Log_Msg_Seq.NEXTVAL, 'TMR_ACQRD_SALES_DATA_LOG', Log_Table_Seq.CURRVAL, 
      DECODE(log_sts_, 'H','H',  'E','N',  'P' ), log_msg_, LD_ID_
   );
      
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;
      
END Insert_Tmr_AcqrdSalesData_Log;



/*-------------------------------------------------------------------------- 
Insert_Tmr_Outgng_Shpmnt_Log (also inserts in LOG_MSG table)
--------------------------------------------------------------------------*/   
PROCEDURE Insert_Tmr_Outgng_Shpmnt_Log  (
   log_sts_                IN            VARCHAR2,
   log_msg_                IN            Log_Msg.Log_Msg%TYPE,
   r                       IN OUT NOCOPY Edm_Outgng_Shpmnt%ROWTYPE, 
   result_                 OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_ITOSL';
   result_ := 0;
   
   INSERT INTO Ans_Sad_Owner.Tmr_Outgng_Shpmnt_Log
   (
      Log_Table_Id, Log_Status,
      Edm_Outgng_Shpmnt_Id,
      Edm_Fclty_Id,             
      Edm_Buyer_Physcl_Addr_Id,    
      Edm_Org_Id_Shpr,           
      Edm_Org_Id_To,          
      Outgng_Shpmnt_Id,      
      Exprtr_Ein_Nbr,                     
      Edm_Prmry_Src_Systm_Cd,
	  Ld_Trnsctn_Dt,
	  Ld_Trnsctn_Usr,
      Batch_Ld_Id
   )
   VALUES
   (
      Log_Table_Seq.NEXTVAL, log_sts_,
      NULL,
      r.Edm_Fclty_Id,             
      r.Edm_Buyer_Physcl_Addr_Id,    
      r.Edm_Org_Id_Shpr,           
      r.Edm_Org_Id_To,          
      r.Outgng_Shpmnt_Id,      
      r.Exprtr_Ein_Nbr,                     
      r.Edm_Prmry_Src_Systm_Cd,
	  SYSDATE,
	  USER,
      LD_ID_    
   );
   
   
   INSERT INTO Log_Msg
   (
      Log_Id, Log_Record_Table, Log_Source_Key, 
      Log_Type, Log_Msg, Batch_Ld_Id 
   ) 
   VALUES
   (
      Log_Msg_Seq.NEXTVAL, 'TMR_OUTGNG_SHPMNT_LOG', Log_Table_Seq.CURRVAL, 
      DECODE(log_sts_, 'H','H',  'E','N',  'P' ), log_msg_, LD_ID_
   );
      
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;
      
END Insert_Tmr_Outgng_Shpmnt_Log;



/*-------------------------------------------------------------------------- 
Insert_Tmr_Cust_Ordr_Log (also inserts in LOG_MSG table)
--------------------------------------------------------------------------*/   
PROCEDURE Insert_Tmr_Cust_Ordr_Log  (
   log_sts_                IN            VARCHAR2,
   log_msg_                IN            Log_Msg.Log_Msg%TYPE,
   r                       IN OUT NOCOPY Edm_Cust_Ordr%ROWTYPE, 
   result_                 OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_ITCOL';
   result_ := 0;
   
   INSERT INTO Ans_Sad_Owner.Tmr_Cust_Ordr_Log
   (
      Log_Table_Id, Log_Status,
      Edm_Cust_Ordr_Id,
      Edm_Org_Id_Buyer ,
      Edm_Crncy_Id     ,
      Cstmr_Ordr_Nbr   ,
      Ordr_Dt,
      Edm_Prmry_Src_Systm_Cd,
	  Ld_Trnsctn_Usr,
	  Ld_Trnsctn_Dt,
      Batch_Ld_Id
   )
   VALUES
   (
      Log_Table_Seq.NEXTVAL, log_sts_,
      NULL,
      r.Edm_Org_Id_Buyer ,
      r.Edm_Crncy_Id     ,
      r.Cstmr_Ordr_Nbr   ,
      r.Ordr_Dt,
      r.Edm_Prmry_Src_Systm_Cd,
	  USER,
	  SYSDATE,
      LD_ID_
   );
   
   
   INSERT INTO Log_Msg
   (
      Log_Id, Log_Record_Table, Log_Source_Key, 
      Log_Type, Log_Msg, Batch_Ld_Id 
   ) 
   VALUES
   (
      Log_Msg_Seq.NEXTVAL, 'TMR_CUST_ORDR_LOG', Log_Table_Seq.CURRVAL, 
      DECODE(log_sts_, 'H','H',  'E','N',  'P' ), log_msg_, LD_ID_
   );
      
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;
      
END Insert_Tmr_Cust_Ordr_Log;


/*-------------------------------------------------------------------------- 
Insert_Tmr_Cust_Ordr_Line_Log (also inserts in LOG_MSG table)
--------------------------------------------------------------------------*/   
PROCEDURE Insert_Tmr_Cust_Ordr_Line_Log  (
   log_sts_                IN            VARCHAR2,
   log_msg_                IN            Log_Msg.Log_Msg%TYPE,
   r                       IN OUT NOCOPY Edm_Cust_Ordr_Line%ROWTYPE, 
   result_                 OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_ITCOLL';
   result_ := 0;
   
   INSERT INTO Ans_Sad_Owner.Tmr_Cust_Ordr_Line_Log
   (
      Log_Table_Id, Log_Status,
      Edm_Cust_Ordr_Line_Id,
      Edm_Buyer_Physcl_Addshipped_To ,
      Edm_Cust_Ordr_Id          ,
      Cstmr_Ordr_Line_Nbr  ,
      Uom_Cd   ,
      Ordr_Qty   ,
   	  Lst_Prc_Amt,
      Net_Slng_Amt,
	  Net_Slng_Amt_Euro, 
      Edm_Fnshd_Prod_Id ,
      Edm_Prmry_Src_Systm_Cd  ,
	  Ld_Trnsctn_Usr,
	  Ld_Trnsctn_Dt,
      Batch_Ld_Id 
   )
   VALUES
   (
      Log_Table_Seq.NEXTVAL, log_sts_,
      NULL,
      r.Edm_Buyer_Physcl_Addshipped_To ,
      r.Edm_Cust_Ordr_Id          ,
      r.Cstmr_Ordr_Line_Nbr  ,
      r.Uom_Cd   ,
      r.Ordr_Qty   ,
   	  r.Lst_Prc_Amt,
      r.Net_Slng_Amt,
	  r.Net_Slng_Amt_Euro ,
      r.Edm_Fnshd_Prod_Id ,
      SRC_SYS_,
	  USER,
	  SYSDATE,
      LD_ID_ 
   );
   
   
   INSERT INTO Log_Msg
   (
      Log_Id, Log_Record_Table, Log_Source_Key, 
      Log_Type, Log_Msg, Batch_Ld_Id 
   ) 
   VALUES
   (
      Log_Msg_Seq.NEXTVAL, 'TMR_CUST_ORDR_LINE_LOG', Log_Table_Seq.CURRVAL, 
      DECODE(log_sts_, 'H','H',  'E','N',  'P' ), log_msg_, LD_ID_
   );
      
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;
      
END Insert_Tmr_Cust_Ordr_Line_Log;


/*-------------------------------------------------------------------------- 
Insert_Tmr_OutgngShpmntLineLog (also inserts in LOG_MSG table)
--------------------------------------------------------------------------*/   
PROCEDURE Insert_Tmr_OutgngShpmntLineLog  (
   log_sts_                IN            VARCHAR2,
   log_msg_                IN            Log_Msg.Log_Msg%TYPE,
   r                       IN OUT NOCOPY Edm_Outgng_Shpmnt_Line%ROWTYPE, 
   result_                 OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_ITOSLL';
   result_ := 0;
   
   INSERT INTO Ans_Sad_Owner.Tmr_Outgng_Shpmnt_Line_Log
   (
      Log_Table_Id, Log_Status,
      Edm_Outgng_Shpmnt_Line_Id,
      Edm_Outgng_Shpmnt_Id   ,
      Edm_Cust_Ordr_Line_Id  ,
      Edm_Fnshd_Prod_Id     ,
      Uom_Cd             ,
      Outgng_Shpmnt_Line_Nbr ,
      Edm_Lot_Id           ,
      Edm_Invntry_Non_Lot_Id   ,
      Edm_Invntry_Lot_Prtn_Id   ,
      Shp_Qty               ,
      Edm_Prmry_Src_Systm_Cd       ,
  	  Ld_Trnsctn_Usr,
	  Ld_Trnsctn_Dt,
      Batch_Ld_Id
   )
   VALUES
   (
      Log_Table_Seq.NEXTVAL, log_sts_,
      NULL,
      r.Edm_Outgng_Shpmnt_Id   ,
      r.Edm_Cust_Ordr_Line_Id  ,
      r.Edm_Fnshd_Prod_Id     ,
      r.Uom_Cd             ,
      r.Outgng_Shpmnt_Line_Nbr ,
      r.Edm_Lot_Id           ,
      r.Edm_Invntry_Non_Lot_Id   ,
      r.Edm_Invntry_Lot_Prtn_Id   ,
      r.Shp_Qty               ,
      SRC_SYS_,
	  USER,
	  SYSDATE,
      LD_ID_
   );
   
   
   INSERT INTO Log_Msg
   (
      Log_Id, Log_Record_Table, Log_Source_Key, 
      Log_Type, Log_Msg, Batch_Ld_Id 
   ) 
   VALUES
   (
      Log_Msg_Seq.NEXTVAL, 'TMR_OUTGNG_SHPMNT_LINE_LOG', Log_Table_Seq.CURRVAL, 
      DECODE(log_sts_, 'H','H',  'E','N',  'P' ), log_msg_, LD_ID_
   );
      
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;
      
END Insert_Tmr_OutgngShpmntLineLog;


/*-------------------------------------------------------------------------- 
Ins_Tmr_InvntryNonLot_Log (also inserts in LOG_MSG table)
--------------------------------------------------------------------------*/   
PROCEDURE Ins_Tmr_InvntryNonLot_Log  (
   log_sts_                IN            VARCHAR2,
   log_msg_                IN            Log_Msg.Log_Msg%TYPE,
   r                       IN OUT NOCOPY Edm_Invntry_Non_Lot%ROWTYPE, 
   result_                 OUT           BINARY_INTEGER )
IS
BEGIN
   MODULE_NM_  := 'TMR_ITINLL';
   result_ := 0;
   
   INSERT INTO Ans_Sad_owner.Tmr_Invntry_Non_Lot_Log
   (
      Log_Table_Id, Log_Status,
      Edm_Invntry_Non_Lot_Id,
      Edm_Fclty_Id,
      Edm_Fnshd_Prod_Id,
      Invntry_As_At_Dt,
      Curr_Qty,
      Invntry_Sts,
      Loc_Desc,
      Edm_Prmry_Src_Systm_Cd,  
  	  Ld_Trnsctn_Usr,
	  Ld_Trnsctn_Dt, 
      Batch_Ld_Id    
   )
   VALUES
   (
      Log_Table_Seq.NEXTVAL, log_sts_,
      NULL,
      r.Edm_Fclty_Id,
      r.Edm_Fnshd_Prod_Id,
      r.Invntry_As_At_Dt,
      r.Curr_Qty,
      r.Invntry_Sts,
      r.Loc_Desc,
      SRC_SYS_,
	  USER,
	  SYSDATE,
      LD_ID_
   );
   
   INSERT INTO Log_Msg
   (
      Log_Id, Log_Record_Table, Log_Source_Key, 
      Log_Type, Log_Msg, Batch_Ld_Id 
   ) 
   VALUES
   (
      Log_Msg_Seq.NEXTVAL, 'TMR_INVNTRY_NON_LOT_LOG', Log_Table_Seq.CURRVAL, 
      DECODE(log_sts_, 'H','H',  'E','N',  'P' ), log_msg_, LD_ID_
   );
      
   
EXCEPTION
   WHEN OTHERS THEN
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      result_ := -1;
      RETURN;
      
END Ins_Tmr_InvntryNonLot_Log;
         

/*-------------------------------------------------------------------------- 
Transform_Consignment_Stock
--------------------------------------------------------------------------*/   
FUNCTION Transform_Consignment_Stock RETURN BINARY_INTEGER IS

   -- 6.3: order by 
   CURSOR cursor_Cstock_LotPrtn IS
      SELECT transaction_date, lot, SUM(qty) SumQty, SUM(qty*list_price_purchase) SumValue 
      FROM ans_sad_owner.tmr_consignment_stock 
      WHERE lot IS NOT NULL AND LENGTH(lot) > 2
      GROUP BY transaction_date, lot
	  HAVING SUM(qty) != 0 OR SUM(qty*list_price_purchase) != 0
	  ORDER BY TO_NUMBER(transaction_date); 
   
   -- 6.3: only one cursor (regardless of lot info) + order by date + not group by   
   CURSOR cursor_Cstock_Product IS
      SELECT transaction_date, nordic_item_number, principal_item_number, qty SumQty, qty*list_price_purchase SumValue, Document_Type, Reason_Code2 
      FROM ans_sad_owner.tmr_consignment_stock 
	  WHERE qty != 0 
	  ORDER BY TO_NUMBER(transaction_date), nordic_item_number;
	  	  
   rec_Ext_Invntry_Mvmnt     Ans_Core_Ext_Owner.Ext_Invntry_Mvmnt%ROWTYPE;
   rec_Edm_Invntry_Lot_Prtn  Edm_Invntry_Lot_Prtn%ROWTYPE;
   rec_Edm_Invntry_Non_Lot   Edm_Invntry_Non_Lot%ROWTYPE;
     
   list_miss_tabs            List_Tables;
   sourcekey                 List_Keys;
   ins_sts_                  BINARY_INTEGER;
   log_sts_                  BINARY_INTEGER;
   upd_sts_                  BINARY_INTEGER;
   xrf_sts_                  BINARY_INTEGER;
   is_master_                BOOLEAN;
   dummy_                    BOOLEAN;
   txt_                      Ans_Sad_Owner.Log_Msg.Log_Msg%TYPE; 
   updated_pk_               NUMBER;
   
BEGIN
   MODULE_NM_   := 'TMR_TCSK';

    FOR rec_Cstock_LotPrtn IN cursor_Cstock_LotPrtn LOOP
    /* 
      Load into EDM_INVNTRY_LOT_PRTN and EXT_INVNTRY_MVMNT  (Transactional)
    */   
         IF CheckMand_Edm_Invntry_Lot_Prtn(rec_Cstock_LotPrtn.SumQty, rec_Cstock_LotPrtn.Transaction_Date, rec_Edm_Invntry_Lot_Prtn) THEN
                 
            GetFk_Edm_Invntry_Lot_Prtn(rec_Cstock_LotPrtn.SumQty, rec_Cstock_LotPrtn.Lot, 'dummy' , rec_Edm_Invntry_Lot_Prtn, list_miss_tabs, sourcekey);
   
            IF list_miss_tabs.COUNT = 0 THEN
               -- Mandatory foreign keys obtained
               -- Eventually, check if this is a DUPLICATE before inserting in EDM!
               rec_Edm_Invntry_Lot_Prtn.Edm_Invntry_Lot_Prtn_Id := Pck_Ans_Util.Get_NextVal('EDM_INVNTRY_LOT_PRTN_SEQ');   
               Insert_Edm_Invntry_Lot_Prtn( rec_Edm_Invntry_Lot_Prtn, ins_sts_, updated_pk_ );
                              
               -- Insert into EXT_INVNTRY_MVMNT 
               IF ins_sts_ = 0 THEN 
                  rec_Ext_Invntry_Mvmnt.Edm_Invntry_Mvmnt_Id    := Pck_Ans_Util.Get_NextVal('ANS_CORE_EXT_OWNER.EXT_INVNTRY_MVMNT_SEQ');
                  rec_Ext_Invntry_Mvmnt.Edm_Invntry_Non_Lot_Id  := NULL;
				  IF updated_pk_ IS NULL THEN
                     rec_Ext_Invntry_Mvmnt.Edm_Invntry_Lot_Prtn_Id := rec_Edm_Invntry_Lot_Prtn.Edm_Invntry_Lot_Prtn_Id;
				  ELSE
				     rec_Ext_Invntry_Mvmnt.Edm_Invntry_Lot_Prtn_Id := updated_pk_;
				  END IF;
                  rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Qty       := rec_Cstock_LotPrtn.SumQty;
                  rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Dt        := rec_Edm_Invntry_Lot_Prtn.Invntry_As_At_Dt;                                 
                  rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Amt       := rec_Cstock_LotPrtn.SumValue;
                  rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Typ_Cd    := 'unknown';                     
                  rec_Ext_Invntry_Mvmnt.Mvmnt_Reason_Cd         := 'unknown';

-- 6.3 THIS IS NOT VERY CORRECT AS EACH CSTK RECORD HAS ITS OWN DOCTYP/RC2
                  SELECT Document_Type, Reason_Code2 
				  INTO rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Typ_Cd, rec_Ext_Invntry_Mvmnt.Mvmnt_Reason_Cd 
                  FROM Ans_Sad_Owner.Tmr_Consignment_Stock
                  WHERE Transaction_Date = rec_Cstock_LotPrtn.Transaction_Date
                    AND   Lot = rec_Cstock_LotPrtn.Lot
                    AND   Document_Type IS NOT NULL
                    AND   ROWNUM = 1;
				
                  Insert_Ext_Invntry_Mvmnt( rec_Ext_Invntry_Mvmnt, ins_sts_ );                                    
               END IF;        
         
            ELSE
               -- At least one mandatory FK not obtained
			   /* V14: LOGGING NOT RELEVANT IN THIS CASE
               txt_ := NULL;
               FOR i IN 1..list_miss_tabs.COUNT LOOP
                  dummy_ := Insert_Xref( SRC_SYS_, SRC_TAB_, sourcekey(i), 'TMR_INVNTRY_LOT_PRTN_LOG', list_miss_tabs(i) , NULL, 'H' );
                  txt_ := ' ' || txt_ || list_miss_tabs(i) || ':' || sourcekey(i);
               END LOOP; 
               Ins_Tmr_InvntryLotPrtn_Log( 'H', 'Missing FKs ' || txt_ , rec_Edm_Invntry_Lot_Prtn, log_sts_ );
			   */  
   			   NULL;
            END IF;
            
         ELSE    
            -- Missing mandatory data
			/* V14: LOGGING NOT RELEVANT IN THIS CASE
            Ins_Tmr_InvntryLotPrtn_Log( 'E', 'Missing mandatory data', rec_Edm_Invntry_Lot_Prtn, log_sts_ );
			*/ 
			NULL;           
         END IF;         
    END LOOP;
    

    FOR rec_Cstock_Product IN cursor_Cstock_Product LOOP
    /* 
      Load into EDM_INVNTRY_NON_LOT and EXT_INVNTRY_MVMNT (Transactional)
    */   
         IF CheckMand_Edm_Invntry_Non_Lot(rec_Cstock_Product.SumQty, rec_Cstock_Product.Transaction_Date, rec_Edm_Invntry_Non_Lot) THEN
                 
            -- Mandatory data OK
            GetFk_Edm_Invntry_Non_Lot(rec_Cstock_Product.SumQty, rec_Cstock_Product.Nordic_Item_Number, rec_Cstock_Product.Principal_Item_Number, 'dummy', rec_Edm_Invntry_Non_Lot, list_miss_tabs, sourcekey);
   
            IF list_miss_tabs.COUNT = 0 THEN
               -- Mandatory foreign keys obtained
               rec_Edm_Invntry_Non_Lot.Edm_Invntry_Non_Lot_Id := Pck_Ans_Util.Get_NextVal('EDM_INVNTRY_NON_LOT_SEQ');   
               Insert_Edm_Invntry_Non_Lot( rec_Edm_Invntry_Non_Lot, ins_sts_, updated_pk_  );
               
               -- Insert into EXT_INVNTRY_MVMNT 
               IF ins_sts_ = 0 THEN 
                  rec_Ext_Invntry_Mvmnt.Edm_Invntry_Mvmnt_Id    := Pck_Ans_Util.Get_NextVal('ANS_CORE_EXT_OWNER.EXT_INVNTRY_MVMNT_SEQ');
				  IF updated_pk_ IS NULL THEN
                     rec_Ext_Invntry_Mvmnt.Edm_Invntry_Non_Lot_Id  := rec_Edm_Invntry_Non_Lot.Edm_Invntry_Non_Lot_Id;
				  ELSE
				     rec_Ext_Invntry_Mvmnt.Edm_Invntry_Non_Lot_Id  := updated_pk_;
				  END IF;
                  rec_Ext_Invntry_Mvmnt.Edm_Invntry_Lot_Prtn_Id := NULL;
                  rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Qty       := rec_Cstock_Product.SumQty;
                  rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Dt        := rec_Edm_Invntry_Non_Lot.Invntry_As_At_Dt;                               
                  rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Amt       := rec_Cstock_Product.SumValue;
                  rec_Ext_Invntry_Mvmnt.Invntry_Mvmnt_Typ_Cd    := rec_Cstock_Product.Document_Type;                     
                  rec_Ext_Invntry_Mvmnt.Mvmnt_Reason_Cd         := rec_Cstock_Product.Reason_Code2;
                                                 
                  Insert_Ext_Invntry_Mvmnt( rec_Ext_Invntry_Mvmnt, ins_sts_ );                  
                  
               END IF;        
            ELSE
               -- At least one mandatory FK not obtained
               txt_ := NULL;
               FOR i IN 1..list_miss_tabs.COUNT LOOP
                  dummy_ := Insert_Xref( SRC_SYS_, SRC_TAB_, sourcekey(i), 'TMR_INVNTRY_NON_LOT_LOG', list_miss_tabs(i) , NULL, 'H' );
                  txt_ := ' ' || txt_ || list_miss_tabs(i) || ':' || sourcekey(i);
               END LOOP; 
               Ins_Tmr_InvntryNonLot_Log( 'H', 'Missing FKs ' || txt_ , rec_Edm_Invntry_Non_Lot, log_sts_ );  
   
            END IF;
            
         ELSE    
            -- Missing mandatory data
            Ins_Tmr_InvntryNonLot_Log( 'E', 'Missing mandatory data', rec_Edm_Invntry_Non_Lot, log_sts_ );           
         END IF;              
       
    END LOOP; 

   RETURN 0; 

EXCEPTION
   WHEN NO_DATA_FOUND THEN
      RETURN 0;
   WHEN OTHERS THEN
      IF cursor_Cstock_LotPrtn%ISOPEN THEN CLOSE cursor_Cstock_LotPrtn; END IF;
      IF cursor_Cstock_Product%ISOPEN THEN CLOSE cursor_Cstock_Product; END IF;
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      RETURN -1;

END Transform_Consignment_Stock;   


/*-------------------------------------------------------------------------- 
Transform_Street_Sales
--------------------------------------------------------------------------*/   
FUNCTION Transform_Street_Sales RETURN BINARY_INTEGER IS

   CURSOR cursor_Ssales IS
	  SELECT s.ROWID SROWID, S.qty SQTY, S.qty*S.LIST_PRICE_SALES SGROSS_VALUE, S.qty*S.LIST_PRICE_PURCHASE SNET_VALUE, S.customer_number_1, S.currency, S.nordic_item_number, S.subgeoreg_1, P.edm_prd_id 
      FROM Ans_Sad_Owner.Tmr_Street_Sales S, Ans_Edm_Owner.Edm_Prd P
      WHERE TO_DATE(S.transaction_date, 'YYYYMMDD') = P.Day_Dt_Strt_On AND P.Prd_Type_Txt = 'CALENDAR_DAYS'; 
	  -- V14. FOR UPDATE OF S.customer_number_1 NOWAIT;

   rec_Edm_AcqrdSalesData    Edm_Acqrd_Sales_Data%ROWTYPE;
   
   list_miss_tabs            List_Tables;
   sourcekey                 List_Keys;
   ins_sts_                  BINARY_INTEGER;
   log_sts_                  BINARY_INTEGER;
   upd_sts_                  BINARY_INTEGER;
   xrf_sts_                  BINARY_INTEGER;
   is_master_                BOOLEAN;
   dummy_                    BOOLEAN;
   txt_                      Ans_Sad_Owner.Log_Msg.Log_Msg%TYPE;
   
BEGIN
   MODULE_NM_   := 'TMR_TSS';
   
   
   -- Get key to seller organization (Tamro AB) 
   SELECT Edm_Org_Id INTO rec_Edm_AcqrdSalesData.Edm_Org_Id_Prvd_By
   FROM Edm_Org
   WHERE Bsns_Nm = TAMRO_SWEDEN_NAME_
   AND ROWNUM = 1;
   
   rec_Edm_AcqrdSalesData.Uom_Cd                  := 'PACKS';
   rec_Edm_AcqrdSalesData.Acqrd_Sales_Data_Typ_Cd := 'STRTSLS'; 
  
   
   FOR rec_Ssales IN cursor_Ssales 
   LOOP
         
      /* 
      Load into EDM_ACQRD_SALES_DATA (Transactional)
      */    
      rec_Edm_AcqrdSalesData.Sold_Qty                := rec_Ssales.SQty;
      rec_Edm_AcqrdSalesData.Lcl_Crncy_Fincl_Val     := rec_Ssales.SNet_Value;
      rec_Edm_AcqrdSalesData.Euro_Fincl_Val          := rec_Ssales.SGross_Value;
         
      GetFk_Edm_AcqrdSalesData( rec_Edm_AcqrdSalesData,
         rec_Ssales.Customer_Number_1,rec_Ssales.Currency,rec_Ssales.Nordic_Item_Number, rec_Ssales.Subgeoreg_1, rec_Ssales.Edm_Prd_Id,  
         list_miss_tabs, sourcekey );
      
      IF list_miss_tabs.COUNT = 0 THEN
         -- Mandatory foreign keys obtained
         rec_Edm_AcqrdSalesData.Edm_Acqrd_Sales_Data_Id  := Pck_Ans_Util.Get_NextVal('EDM_ACQRD_SALES_DATA_SEQ');   
         Insert_Edm_AcqrdSalesData( rec_Edm_AcqrdSalesData, ins_sts_ ); -- Insert-Else-Update
		 IF ins_sts_ = 0 THEN 
		    -- Source record loaded, delete it from SAD table
		    DELETE FROM Ans_Sad_Owner.Tmr_Street_Sales WHERE ROWID = rec_Ssales.SROWID; -- V14
		 END IF;
      ELSE
         -- At least one mandatory FK not obtained
         txt_ := NULL;
         FOR i IN 1..list_miss_tabs.COUNT LOOP
            dummy_ := Insert_Xref( SRC_SYS_, SRC_TAB_, sourcekey(i),'TMR_ACQRD_SALES_DATA_LOG', list_miss_tabs(i), NULL, 'H' );
            txt_ := ' ' || txt_ || list_miss_tabs(i) || ':' || sourcekey(i);
         END LOOP; 
		 -- Insert_Tmr_Street_Sales_Log('H', 'Missing FKs ' || txt_, rec_Ssales, log_sts_ ); 
         Insert_Tmr_AcqrdSalesData_Log( 'H', 'Missing FKs ' || txt_, rec_Edm_AcqrdSalesData, log_sts_ );
      END IF;
    
	  IF MOD(cursor_Ssales%ROWCOUNT,  100000) = 0 THEN COMMIT; END IF; -- V14
	                
   END LOOP;
   
   COMMIT; -- V14;
   
   RETURN 0;

EXCEPTION
   WHEN OTHERS THEN
      IF cursor_Ssales%ISOPEN THEN CLOSE cursor_Ssales; END IF;
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      RETURN -1;

END Transform_Street_Sales;   



/*-------------------------------------------------------------------------- 
Transform_Consignment_Sales
--------------------------------------------------------------------------*/   
FUNCTION Transform_Consignment_Sales RETURN BINARY_INTEGER IS

   CURSOR cursor_Csales IS
      SELECT * FROM Ans_Sad_Owner.Tmr_Consignment_Sales; -- 6.2 ORDER BY Order_Number;
   
   rec_Edm_Cust_Ordr           Edm_Cust_Ordr%ROWTYPE;
   rec_Edm_Cust_Ordr_Line      Edm_Cust_Ordr_Line%ROWTYPE;
   rec_Edm_Outgng_Shpmnt       Edm_Outgng_Shpmnt%ROWTYPE;      
   rec_Edm_Outgng_Shpmnt_Line  Edm_Outgng_Shpmnt_Line%ROWTYPE;
      
   list_miss_tabs            List_Tables;
   sourcekey                 List_Keys;
   ins_sts_                  BINARY_INTEGER;
   log_sts_                  BINARY_INTEGER;
   upd_sts_                  BINARY_INTEGER;
   xrf_sts_                  BINARY_INTEGER;
   is_master_                BOOLEAN;
   previous_ordernumber_     Ans_Sad_Owner.Tmr_Consignment_Sales.Order_Number%TYPE;
   current_ordernumber_      Ans_Sad_Owner.Tmr_Consignment_Sales.Order_Number%TYPE;
   outgng_shpmnt_id_         NUMBER;
   cust_ordr_id_             NUMBER;
   dummy_                    BOOLEAN;
   txt_                      Ans_Sad_Owner.Log_Msg.Log_Msg%TYPE;
     
BEGIN
   MODULE_NM_   := 'TMR_TCSL';
   OPEN cursor_Csales;
   
   previous_ordernumber_ := -1;
    
   LOOP
      FETCH cursor_Csales INTO rec_Csales;
      EXIT WHEN cursor_Csales%NOTFOUND;
      
      current_ordernumber_ := rec_Csales.Order_Number;
          
      /* 
      Load into EDM_CUST_ORDR and EDM_OUTGNG_SHPMNT (Transaction): 
      for every distinct value of rec_Csales.Order_Number 
      */
      previous_ordernumber_ := current_ordernumber_;
cust_ordr_id_         := NULL;
outgng_shpmnt_id_     := NULL;

      IF CheckMand_Edm_Cust_Ordr(rec_Edm_Cust_Ordr) THEN
      
         -- Mandatory data OK
         GetFk_Edm_Cust_Ordr(rec_Edm_Cust_Ordr, list_miss_tabs, sourcekey );
         
         IF list_miss_tabs.COUNT = 0 THEN
            -- Mandatory foreign keys obtained
            rec_Edm_Cust_Ordr.Edm_Cust_Ordr_Id  := Pck_Ans_Util.Get_NextVal('EDM_CUST_ORDR_SEQ');
   rec_Edm_Cust_Ordr.Cstmr_Ordr_Nbr    := SUBSTR(TO_CHAR(rec_Edm_Cust_Ordr.Edm_Cust_Ordr_Id), 1, 20);
            Insert_Edm_Cust_Ordr( rec_Edm_Cust_Ordr, ins_sts_ );
            IF ins_sts_ = 0 THEN
               cust_ordr_id_   := rec_Edm_Cust_Ordr.Edm_Cust_Ordr_Id;
            END IF;              
         ELSE
            -- At least one mandatory FK not obtained
            txt_ := NULL;
            FOR i IN 1..list_miss_tabs.COUNT LOOP
               dummy_ := Insert_Xref( SRC_SYS_, SRC_TAB_, sourcekey(i), 'TMR_CUST_ORDR_LOG', list_miss_tabs(i), NULL, 'H' );
               txt_ := ' ' || txt_ || list_miss_tabs(i) || ':' || sourcekey(i);
            END LOOP; 
            Insert_Tmr_Cust_Ordr_Log( 'H', 'Missing FKs ' || txt_, rec_Edm_Cust_Ordr, log_sts_ );
         END IF;
         
      ELSE         
         -- Missing mandatory data
         Insert_Tmr_Cust_Ordr_Log( 'E', 'Missing mandatory data', rec_Edm_Cust_Ordr, log_sts_ );          
      END IF;

      
      IF CheckMand_Edm_Outgng_Shpmnt(rec_Edm_Outgng_Shpmnt) THEN
      
         -- Mandatory data OK
         GetFk_Edm_Outgng_Shpmnt(rec_Edm_Outgng_Shpmnt, list_miss_tabs, sourcekey );
         
         IF list_miss_tabs.COUNT = 0 THEN
            -- Mandatory foreign keys obtained
            rec_Edm_Outgng_Shpmnt.Edm_Outgng_Shpmnt_Id  := Pck_Ans_Util.Get_NextVal('EDM_OUTGNG_SHPMNT_SEQ');
            rec_Edm_Outgng_Shpmnt.Outgng_Shpmnt_Id := SUBSTR(TO_CHAR(rec_Edm_Cust_Ordr.Edm_Cust_Ordr_Id), 1, 10);   
            Insert_Edm_Outgng_Shpmnt( rec_Edm_Outgng_Shpmnt, ins_sts_ );
            IF ins_sts_ = 0 THEN 
               outgng_shpmnt_id_   := rec_Edm_Outgng_Shpmnt.Edm_Outgng_Shpmnt_Id;
            END IF;               
         ELSE
            -- At least one mandatory FK not obtained
            txt_ := NULL;
            FOR i IN 1..list_miss_tabs.COUNT LOOP
               dummy_ := Insert_Xref( SRC_SYS_, SRC_TAB_, sourcekey(i), 'TMR_OUTGNG_SHPMNT_LOG', list_miss_tabs(i), NULL, 'H' );
               txt_ := ' ' || txt_ || list_miss_tabs(i) || ':' || sourcekey(i);
            END LOOP;               
            Insert_Tmr_Outgng_Shpmnt_Log( 'H', 'Missing FKs ' || txt_ , rec_Edm_Outgng_Shpmnt, log_sts_ );
         END IF;
         
      ELSE
         -- Missing mandatory data
         Insert_Tmr_Outgng_Shpmnt_Log( 'E', 'Missing mandatory data', rec_Edm_Outgng_Shpmnt, log_sts_ );             
      END IF;
   
         
      /* 
      Load into EDM_CUST_ORDR_LINE (Transaction) 
      */
      rec_Edm_Cust_Ordr_Line.Edm_Cust_Ordr_Line_Id := NULL;
      IF CheckMand_Edm_Cust_Ordr_Line(rec_Edm_Cust_Ordr_Line) THEN
      
         -- Mandatory data OK
         GetFk_Edm_Cust_Ordr_Line(rec_Edm_Cust_Ordr_Line, cust_ordr_id_, list_miss_tabs, sourcekey );
         
         IF list_miss_tabs.COUNT = 0 THEN
            -- Mandatory foreign keys obtained
            rec_Edm_Cust_Ordr_Line.Edm_Cust_Ordr_Line_Id  := Pck_Ans_Util.Get_NextVal('EDM_CUST_ORDR_LINE_SEQ');
			rec_Edm_Cust_Ordr_Line.Cstmr_Ordr_Line_Nbr := rec_Edm_Cust_Ordr_Line.Edm_Cust_Ordr_Line_Id;   
            Insert_Edm_Cust_Ordr_Line( rec_Edm_Cust_Ordr_Line, ins_sts_ );         
         ELSE
            -- At least one mandatory FK not obtained
            txt_ := NULL;
            FOR i IN 1..list_miss_tabs.COUNT LOOP
               dummy_ := Insert_Xref( SRC_SYS_, SRC_TAB_, sourcekey(i), 'TMR_CUST_ORDR_LINE_LOG', list_miss_tabs(i), NULL, 'H' );
               txt_ := ' ' || txt_ || list_miss_tabs(i) || ':' || sourcekey(i);
            END LOOP;             
            Insert_Tmr_Cust_Ordr_Line_Log( 'H', 'Missing FKs ' || txt_, rec_Edm_Cust_Ordr_Line, log_sts_ );           
         END IF;
         
      ELSE
         -- Missing mandatory data
         Insert_Tmr_Cust_Ordr_Line_Log( 'E', 'Missing mandatory data', rec_Edm_Cust_Ordr_Line, log_sts_ );     
             
      END IF;  
      

      /* 
      Load into EDM_OUTGNG_SHPMNT_LINE (Transaction) 
      */
      IF CheckMand_EdmOutgngShpmntLine(rec_Edm_Outgng_Shpmnt_Line) THEN
      
         -- Mandatory data OK
         GetFk_Edm_Outgng_Shpmnt_Line(rec_Edm_Cust_Ordr_Line.Edm_Cust_Ordr_Line_Id, rec_Edm_Outgng_Shpmnt_Line, outgng_shpmnt_id_, list_miss_tabs, sourcekey );
         
         IF list_miss_tabs.COUNT = 0 THEN
            -- Mandatory foreign keys obtained
            rec_Edm_Outgng_Shpmnt_Line.Edm_Outgng_Shpmnt_Line_Id  := Pck_Ans_Util.Get_NextVal('EDM_OUTGNG_SHPMNT_LINE_SEQ');
            rec_Edm_Outgng_Shpmnt_Line.Outgng_Shpmnt_Line_Nbr := rec_Edm_Cust_Ordr_Line.Edm_Cust_Ordr_Line_Id;
            Insert_Edm_Outgng_Shpmnt_Line( rec_Edm_Outgng_Shpmnt_Line, ins_sts_ );           
         ELSE
            -- At least one mandatory FK not obtained
            txt_ := NULL;
            FOR i IN 1..list_miss_tabs.COUNT LOOP
               dummy_ := Insert_Xref( SRC_SYS_, SRC_TAB_, sourcekey(i),'TMR_OUTGNG_SHPMNT_LINE_LOG', list_miss_tabs(i), NULL, 'H' );
               txt_ := ' ' || txt_ || list_miss_tabs(i) || ':' || sourcekey(i);
            END LOOP; 
            Insert_Tmr_OutgngShpmntLineLog( 'H', 'Missing FKs ' || txt_ , rec_Edm_Outgng_Shpmnt_Line, log_sts_ );             
         END IF;
         
      ELSE
      
         -- Missing mandatory data
         Insert_Tmr_OutgngShpmntLineLog( 'E', 'Missing mandatory data', rec_Edm_Outgng_Shpmnt_Line, log_sts_ );       
             
      END IF;  
      
      
   END LOOP;
   CLOSE cursor_Csales;
   
   RETURN 0;

EXCEPTION
   WHEN OTHERS THEN
      IF cursor_Csales%ISOPEN THEN CLOSE cursor_Csales; END IF;
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      RETURN -1;

END Transform_Consignment_Sales;


/*-------------------------------------------------------------------------- 
Transform_Lot
--------------------------------------------------------------------------*/   
FUNCTION Transform_Lot RETURN BINARY_INTEGER IS
	  
   CURSOR cursor_Cstock_Lot IS
      SELECT * 
      FROM Ans_Flat_Owner.Tmr_Tamro
      WHERE lot IS NOT NULL AND LENGTH(lot) > 1
      ORDER BY lot;

   rec_Cstock_Lot        Ans_Flat_Owner.Tmr_Tamro%ROWTYPE;
   rec_Edm_Lot           Edm_Lot%ROWTYPE;
   
   list_miss_tabs            List_Tables;
   sourcekey                 List_Keys;
   ins_sts_                  BINARY_INTEGER;
   log_sts_                  BINARY_INTEGER;
   upd_sts_                  BINARY_INTEGER;
   xrf_sts_                  BINARY_INTEGER;
   is_master_                BOOLEAN;
   dummy_                    BOOLEAN;
   txt_                      Ans_Sad_Owner.Log_Msg.Log_Msg%TYPE; 
   previous_lot_             Ans_Flat_Owner.Tmr_Tamro.Lot%TYPE;

BEGIN
   MODULE_NM_   := 'TMR_TL';
      	  
   /* 
   Load into EDM_LOT (Referential) 
   */   
   OPEN cursor_Cstock_Lot;
   
   previous_lot_ := '';
   LOOP
      
      FETCH cursor_Cstock_Lot INTO rec_Cstock_Lot;
      EXIT WHEN cursor_Cstock_Lot%NOTFOUND;
      
      rec_Edm_Lot.Edm_Lot_Id := NULL;
      
      IF rec_Cstock_Lot.Lot = previous_lot_ THEN
         NULL;
      ELSE
         previous_lot_  := rec_Cstock_Lot.Lot;
         
         rec_Edm_Lot.Lot_Id_Cd := rec_Cstock_Lot.Lot;
         is_master_  := TRUE;  -- Set to TRUE or FALSE depending on the relationship 
                               -- between the source system and EDM_LOT
                            
         Exists_Xref( SRC_SYS_, rec_Cstock_Lot.Lot, 'EDM_LOT', rec_Edm_Lot.Edm_Lot_Id, xrf_sts_ );
      
         IF xrf_sts_ = 0 THEN  -- Not Found (new entity)
                                
            IF CheckMand_Edm_Lot(rec_Cstock_Lot.Expiry_Date, rec_Edm_Lot) THEN
               -- Mandatory data OK
               
               GetFk_Edm_Lot( rec_Cstock_Lot.Nordic_Item_Number, rec_Cstock_Lot.Principal_Item_Number, 
                              rec_Edm_Lot, list_miss_tabs );
               
               IF list_miss_tabs.COUNT = 0 THEN
                  -- Mandatory foreign keys obtained => Insert in Xref and EDM
                  rec_Edm_Lot.Edm_Lot_Id  := Pck_Ans_Util.Get_NextVal('EDM_LOT_SEQ');
                  IF Insert_Xref( SRC_SYS_, SRC_TAB_, rec_Cstock_Lot.Lot, NULL, 'EDM_LOT', rec_Edm_Lot.Edm_Lot_Id, 'V' ) THEN     
                     Insert_Edm_Lot( rec_Edm_Lot, ins_sts_ ); 
                     IF ins_sts_ = 0 THEN   
                         Validate_Xref_Flag( 'EDM_LOT', rec_Edm_Lot.Edm_Lot_Id, 'V' );
                     END IF;
                  END IF;
                  
               ELSE
                  -- At least one mandatory FK not obtained
                  FOR i IN 1..list_miss_tabs.COUNT LOOP
                     dummy_ := Insert_Xref( SRC_SYS_, SRC_TAB_, TRIM(TO_CHAR(rec_Cstock_Lot.Nordic_Item_Number, '099999')),
                                            'TMR_LOT_LOG', list_miss_tabs(i), NULL, 'H' );
                  END LOOP;  
                  Insert_Tmr_Lot_Log( 'H', 'Missing FKs ' || list_miss_tabs(1) || ':' || TO_CHAR(rec_Cstock_Lot.Nordic_Item_Number, '099999')
				  , rec_Edm_Lot, log_sts_ );                 
                   
               END IF;                
            ELSE          
               -- Missing mandatory data
               Insert_Tmr_Lot_Log( 'E', 'Missing mandatory data', rec_Edm_Lot, log_sts_ );                                 
            END IF;
             
         ELSIF xrf_sts_ = 1 THEN  -- Found (entity already exists in EDM) 
             
            IF is_master_ THEN
               IF CheckMand_Edm_Lot(rec_Cstock_Lot.Expiry_Date, rec_Edm_Lot) THEN
                  -- Mandatory data OK
                  GetFk_Edm_Lot(rec_Cstock_Lot.Nordic_Item_Number, rec_Cstock_Lot.Principal_Item_Number, 
                                rec_Edm_Lot, list_miss_tabs );
                  IF list_miss_tabs.COUNT = 0 THEN
                     Update_Edm_Lot( rec_Edm_Lot, upd_sts_ );
                     Validate_Xref_Flag( 'EDM_LOT', rec_Edm_Lot.Edm_Lot_Id, 'V' );
                  END IF;
               END IF;
            END IF;
     
         END IF;
         
      END IF; -- If current equals previous
      
   END LOOP;
   CLOSE cursor_Cstock_Lot;

   RETURN 0;

EXCEPTION
   WHEN OTHERS THEN
      IF cursor_Cstock_Lot%ISOPEN THEN CLOSE cursor_Cstock_Lot; END IF;
      Event_Log.log_event(SQLERRM, MODULE_NM_, 'E');
      RETURN -1;
	  	  
END Transform_Lot;


/*-------------------------------------------------------------------------- 
Transform_CifPrices
--------------------------------------------------------------------------*/   
FUNCTION Transform_CifPrices RETURN BINARY_INTEGER IS
	  
   CURSOR cursor_TamroPrices IS 
      SELECT DISTINCT TO_DATE(t.TRANSACTION_DATE, 'YYYYMMDD') Tamro_Date, 
                      x.XREF_ANSWERS_ID                       Product_Edm_Id, 
      				  c.edm_crncy_id                          Currency_Edm_Id, 
      				  t.LIST_PRICE_PURCHASE                   Tamro_Lpp
      FROM ans_flat_owner.tmr_tamro t, ans_sad_owner.xrf_xreference x, ans_edm_owner.edm_crncy c 
      WHERE
      ( x.XREF_SOURCE_SYSTEM = 'CONCORDE' AND x.HOLD_FLAG IN ('L','V') AND x.XREF_ANSWERS_TABLE = 'EDM_FNSHD_PROD' AND x.XREF_SOURCE_KEY = TRIM(TO_CHAR(t.NORDIC_ITEM_NUMBER, '099999')) )
      AND
      ( c.crncy_cd = t.CURRENCY )
      AND
      (
        ( 
          DOCUMENT_TYPE IN ('OP','O4','O1','T1','TU','TP','CN','SA','SG','SO', 'TE','CO','OS','S5','S8','I1','I2','IA','IE','IF','IG','IH','IO')
        )
        OR
        ( 
          Document_Type IN ('SO','SA','SF','SG','SD','CO','CR','CU','TE') AND Qty != 0
        )
        OR
        (
             (
             DOCUMENT_TYPE IN ('T1','TU') AND OWNER = 'P'
             AND CUSTOMER_NUMBER_1 IN (3384, 3385, 3386, 3387)
             )
             OR
             (
             DOCUMENT_TYPE IN ('T1','T8')
             AND OWNER = 'T'
             AND CUSTOMER_NUMBER_1 IN (3389, 3390, 3391)
             )
             OR
             (
             DOCUMENT_TYPE = 'T9'
             AND OWNER = 'T'
             )
             OR
             (
             DOCUMENT_TYPE = 'I1'
             AND OWNER = 'P'
             AND REASON_CODE LIKE 'W%'
             )
             OR
             (
             DOCUMENT_TYPE = 'CR'
             AND OWNER = 'P'
             AND REASON_CODE IN ('R02','R03','R04','R05','R11')
             )
             OR
             (
             DOCUMENT_TYPE IN ('IF','S8','SA','SO','SD','SG','CO','PI','IM','TA')
             AND OWNER = 'P'
             )
             OR
             ( REASON_CODE = 'I21' )
        )
      )
	  ORDER BY Product_Edm_Id, Tamro_Date;

	  
   rec_Edm_Item_Prc           Edm_Item_Prc%ROWTYPE;
   step_                      CHAR(1);
   ins_sts_                   BINARY_INTEGER;

   previous_product_  		  ans_sad_owner.xrf_xreference.XREF_ANSWERS_ID%TYPE;
   previous_lpp_      		  ans_flat_owner.tmr_tamro.list_price_purchase%TYPE;
   
BEGIN
   MODULE_NM_   := 'TMR_TCP';   	  
   /* 
   Load into EDM_ITEM_PRC (Transactional) 
   */   
   
   -- Input-independent values
   step_ := '0';
   SELECT Edm_Gpltcl_Area_Id INTO rec_Edm_Item_Prc.Edm_Gpltcl_Area_Id
   FROM Edm_Gpltcl_Area
   WHERE Iso_Cd = 'SE' AND Edm_Geo_Pltcl_Area_Type_Cd = 'COUNTRY' AND ROWNUM = 1;
   
   rec_Edm_Item_Prc.Prc_Typ_Cd             := 'CIF';
   rec_Edm_Item_Prc.Org_Typ_Cd             := 'WLSR';
   rec_Edm_Item_Prc.Edm_Bsns_Grp_Id        := NULL;
   rec_Edm_Item_Prc.Edm_Prmry_Src_Systm_Cd := SRC_SYS_;
   rec_Edm_Item_Prc.Edm_Batch_Ld_Id        := LD_ID_;
   rec_Edm_Item_Prc.Edm_Mrkt_Prod_Grp_Id   := NULL;
   rec_Edm_Item_Prc.Prc_Sts   			   := 'A';
   
   previous_product_  := -1;
   previous_lpp_      := -1;
   FOR rec_TamroPrices IN cursor_TamroPrices 
   LOOP
       rec_Edm_Item_Prc.Edm_Crncy_Id       := rec_TamroPrices.Currency_Edm_Id;
       rec_Edm_Item_Prc.Edm_Fnshd_Prod_Id  := rec_TamroPrices.Product_Edm_Id;
       rec_Edm_Item_Prc.Efctv_Dt           := rec_TamroPrices.Tamro_Date;
       rec_Edm_Item_Prc.Unt_Prc            := rec_TamroPrices.Tamro_Lpp;
	    
	   IF rec_TamroPrices.Product_Edm_Id = previous_product_ THEN
	   
	      IF rec_TamroPrices.Tamro_Lpp = previous_lpp_ THEN
		  
		     NULL;
			 
		  ELSE
		  
		     previous_lpp_  := rec_TamroPrices.Tamro_Lpp;

	 	     -- set sts = 'I' on all rows found in EDM_ITEM_PRC (if any)
		     UPDATE Ans_Edm_Owner.Edm_Item_Prc 
	    	 SET    Prc_Sts = 'I'
	    	 WHERE  Edm_Fnshd_Prod_Id = rec_TamroPrices.Product_Edm_Id 
		   	   AND  Efctv_Dt < rec_TamroPrices.Tamro_Date;
				  
			 Insert_Edm_Item_Prc (rec_Edm_Item_Prc, ins_sts_ );
			   	 
		  END IF;
		  
	   ELSE
	
		 previous_lpp_  := rec_TamroPrices.Tamro_Lpp;
		 
	     -- set sts = 'I' on all OLDER rows found in EDM_ITEM_PRC (if any) 
	     UPDATE Ans_Edm_Owner.Edm_Item_Prc 
	     SET    Prc_Sts = 'I'
	     WHERE  Edm_Fnshd_Prod_Id = rec_TamroPrices.Product_Edm_Id 
		    AND Efctv_Dt < rec_TamroPrices.Tamro_Date;
		 
         Insert_Edm_Item_Prc (rec_Edm_Item_Prc, ins_sts_ );
		  
	   END IF;
	   previous_product_  := rec_TamroPrices.Product_Edm_Id;
	   
	   IF MOD(cursor_TamroPrices%ROWCOUNT,  1000) = 0 THEN COMMIT; END IF;
	   	   	   
   END LOOP;

   COMMIT;
   
   RETURN 0;

EXCEPTION
   WHEN OTHERS THEN
      IF cursor_TamroPrices%ISOPEN THEN CLOSE cursor_TamroPrices; END IF;
      Event_Log.log_event(SQLERRM, MODULE_NM_ || '#' || step_, 'E');
      RETURN -1;
	  	  
END Transform_CifPrices;



/* -------------------------------------------------------------------------- 
Remove_Invalid_Flat_Records
-------------------------------------------------------------------------- */
FUNCTION Remove_Invalid_Flat_Records RETURN BINARY_INTEGER IS

BEGIN

   DELETE FROM Ans_Flat_Owner.Tmr_Tamro WHERE Start_Tag <> 'TMR';
   INVALID_REMOVED_ := TRUE;
   RETURN 0;

EXCEPTION
   WHEN OTHERS THEN RETURN -1;

END Remove_Invalid_Flat_Records;



/* -------------------------------------------------------------------------- 
Extract_Consignment_Stock
-------------------------------------------------------------------------- */
FUNCTION Extract_Consignment_Stock RETURN BINARY_INTEGER IS

   CURSOR cursor_tmr_cstk IS
   SELECT * FROM Ans_Sad_Owner.Tmr_Consignment_Stock FOR UPDATE;
   
   trs_ec_               Ans_Sad_Owner.Tmr_Translation.EVENT_CODE%TYPE;
   trs_et_               Ans_Sad_Owner.Tmr_Translation.EVENT_TYPE%TYPE;
   trs_rc2_              Ans_Sad_Owner.Tmr_Translation.REASON_CODE2%TYPE;
   trs_qty_multiplier_   Ans_Sad_Owner.Tmr_Translation.Cstock_Qty%TYPE;


BEGIN
   MODULE_NM_ := 'TMR_ECSTK';
                  
   DELETE FROM Ans_Sad_Owner.Tmr_Consignment_Stock;  -- TRUNCATE would be better

   -- Select from flat area
   INSERT INTO Ans_Sad_Owner.Tmr_Consignment_Stock
    SELECT 
    -- Columns common with Tmr_Tamro
    Start_Tag,
    Transaction_Date,
    Principal_Number,
    Item_Number,
    Nordic_Item_Number,
    Principal_Item_Number ,
    Ean_Code,
    Customer_Number_1,
    Georeg_1,
    Subgeoreg_1,
    Country_1,
    Customer_Number_2,
    Georeg_2,
    Subgeoreg_2,
    Country_2,
    Warehouse,
    Transaction_Type,
    Simplified_Document_Type,
    Document_Type ,
    Reason_Code ,
    Return_Reason  ,
    Order_Date   ,
    Order_Number ,
    Order_Line ,
    Customer_Order_Number  ,
    Principals_Order_Number ,
    Currency ,
    Owner  ,
    Lot  ,
    Expiry_Date  ,
    Lot_Status ,
    Qty  ,
    Backorder_Date ,
    Gross_Value ,
    Discount,
    Net_Value,
    Vat_Value  ,
    Vat_Percentage ,
    List_Price_Purchase ,
    List_Price_Sales,
    Product_Class,
    -- Extra fields
    NULL,
    NULL,
    NULL
   FROM Ans_Flat_Owner.Tmr_Tamro
   WHERE
      DOCUMENT_TYPE IN ('OP','O4','O1','T1','TU','TP','CN','SA','SG','SO',
           'TE','CO','OS','S5','S8','I1','I2','IA','IE','IF','IG','IH','IO');

           
   -- Update according to specification      
   FOR rec_tmr_cstk IN cursor_tmr_cstk -- all records
   LOOP
   
      BEGIN -- BEGIN autonomous block to handle in-loop exceptions
      
         IF rec_tmr_cstk.Reason_Code IS NULL THEN
         
            SELECT B.Event_Code, B.Event_Type, B.Reason_Code2, B.Cstock_Qty
            INTO trs_ec_, trs_et_, trs_rc2_, trs_qty_multiplier_
            FROM Ans_Sad_Owner.Tmr_Translation B
            WHERE B.Doc_Type  = rec_tmr_cstk.Document_Type
              AND B.Owner = rec_tmr_cstk.Owner
              AND B.Reason_Code IS NULL
              AND ROWNUM = 1;
              
            UPDATE Ans_Sad_Owner.Tmr_Consignment_Stock
            SET Event_Code = trs_ec_, Event_Type = trs_et_, Reason_Code2 = trs_rc2_ , Qty = Qty * trs_qty_multiplier_, 
			    Gross_Value = Gross_Value * trs_qty_multiplier_, 
        	    Net_Value   = Net_Value   * trs_qty_multiplier_,
        	    Vat_Value   = Vat_Value   * trs_qty_multiplier_,
        	    Discount    = Discount    * trs_qty_multiplier_				
            WHERE CURRENT OF cursor_tmr_cstk;
             
         ELSE 
         
            SELECT B.Event_Code, B.Event_Type, B.Reason_Code2, B.Cstock_Qty
            INTO trs_ec_, trs_et_, trs_rc2_, trs_qty_multiplier_
            FROM Ans_Sad_Owner.Tmr_Translation B
            WHERE B.Doc_Type  = rec_tmr_cstk.Document_Type
              AND B.Owner = rec_tmr_cstk.Owner
              AND B.Reason_Code = rec_tmr_cstk.Reason_Code
              AND ROWNUM = 1;
              
            UPDATE Ans_Sad_Owner.Tmr_Consignment_Stock
            SET Event_Code = trs_ec_, Event_Type = trs_et_, Reason_Code2 = trs_rc2_ , Qty = Qty * trs_qty_multiplier_,
			    Gross_Value = Gross_Value * trs_qty_multiplier_, 
        	    Net_Value   = Net_Value   * trs_qty_multiplier_,
        	    Vat_Value   = Vat_Value   * trs_qty_multiplier_,
        	    Discount    = Discount    * trs_qty_multiplier_
            WHERE CURRENT OF cursor_tmr_cstk;
         
         END IF;
      
      EXCEPTION
         WHEN NO_DATA_FOUND THEN -- continue 
            Event_Log.Log_Event('No Translation for: ' || rec_tmr_cstk.Document_Type || rec_tmr_cstk.Owner || rec_tmr_cstk.Reason_Code ,MODULE_NM_,'W');      
         WHEN OTHERS THEN 
            RAISE;
            
      END; -- END autonomous block
   
   END LOOP;
   
   RETURN 0;

EXCEPTION
   WHEN OTHERS THEN RETURN -1;

END Extract_Consignment_Stock;


/* -------------------------------------------------------------------------- 
Extract_Street_Sales
-------------------------------------------------------------------------- */
FUNCTION Extract_Street_Sales RETURN BINARY_INTEGER IS

   CURSOR cursor_tmr_ssal IS
   SELECT * FROM Ans_Sad_Owner.Tmr_Street_Sales FOR UPDATE;
   
   trs_ec_               Ans_Sad_Owner.Tmr_Translation.EVENT_CODE%TYPE;
   trs_et_               Ans_Sad_Owner.Tmr_Translation.EVENT_TYPE%TYPE;
   trs_rc2_              Ans_Sad_Owner.Tmr_Translation.REASON_CODE2%TYPE;
   trs_qty_multiplier_   Ans_Sad_Owner.Tmr_Translation.Ssales_Qty%TYPE;

BEGIN
   MODULE_NM_ := 'TMR_ESS';
   
   DELETE FROM Ans_Sad_Owner.Tmr_Street_Sales;  -- TRUNCATE would be better

   -- Select from flat area

   INSERT INTO Ans_Sad_Owner.Tmr_Street_Sales
   SELECT
    -- Columns common with Tmr_Tamro
    Start_Tag,
    Transaction_Date,
    Principal_Number,
    Item_Number,
    Nordic_Item_Number,
    Principal_Item_Number ,
    Ean_Code,
    Customer_Number_1,
    Georeg_1,
    Subgeoreg_1,
    Country_1,
    Customer_Number_2,
    Georeg_2,
    Subgeoreg_2,
    Country_2,
    Warehouse,
    Transaction_Type,
    Simplified_Document_Type,
    Document_Type ,
    Reason_Code ,
    Return_Reason  ,
    Order_Date   ,
    Order_Number ,
    Order_Line ,
    Customer_Order_Number  ,
    Principals_Order_Number ,
    Currency ,
    Owner  ,
    Lot  ,
    Expiry_Date  ,
    Lot_Status ,
    Qty  ,
    Backorder_Date ,
    Gross_Value ,
    Discount,
    Net_Value,
    Vat_Value  ,
    Vat_Percentage ,
    List_Price_Purchase ,
    List_Price_Sales,
    Product_Class,
    -- Extra Fields
    NULL,
    NULL,
    NULL
   FROM Ans_Flat_Owner.Tmr_Tamro
   WHERE
	 (
	 Document_Type IN ('SO','SA','SF','SG','SD','CO','CR','CU','TE')
	 AND Qty <> 0
	 );

     
   -- Update according to specification     
   FOR rec_tmr_ssal IN cursor_tmr_ssal -- all records
   LOOP
         BEGIN -- BEGIN autonomous block to handle in-loop exceptions
         
            IF rec_tmr_ssal.Reason_Code IS NULL THEN
            
    		   SELECT B.Event_Code, B.Event_Type, B.Reason_Code2, B.Ssales_Qty
               INTO trs_ec_, trs_et_, trs_rc2_, trs_qty_multiplier_
               FROM Ans_Sad_Owner.Tmr_Translation B
               WHERE B.Doc_Type  = rec_tmr_ssal.Document_Type
                 AND B.Owner = rec_tmr_ssal.Owner
                 AND B.Reason_Code IS NULL
                 AND ROWNUM = 1;
                 
               UPDATE Ans_Sad_Owner.Tmr_Street_Sales
               SET Event_Code = trs_ec_, Event_Type = trs_et_, Reason_Code2 = trs_rc2_ , Qty = Qty * trs_qty_multiplier_,
   			    Gross_Value = Gross_Value * trs_qty_multiplier_, 
        	    Net_Value   = Net_Value   * trs_qty_multiplier_,
        	    Vat_Value   = Vat_Value   * trs_qty_multiplier_,
        	    Discount    = Discount    * trs_qty_multiplier_
               WHERE CURRENT OF cursor_tmr_ssal;
                
            ELSE 
            
    		   SELECT B.Event_Code, B.Event_Type, B.Reason_Code2, B.Ssales_Qty
               INTO trs_ec_, trs_et_, trs_rc2_, trs_qty_multiplier_
               FROM Ans_Sad_Owner.Tmr_Translation B
               WHERE B.Doc_Type  = rec_tmr_ssal.Document_Type
                 AND B.Owner = rec_tmr_ssal.Owner
                 AND B.Reason_Code = rec_tmr_ssal.Reason_Code
                 AND ROWNUM = 1;
                 
               UPDATE Ans_Sad_Owner.Tmr_Street_Sales
               SET Event_Code = trs_ec_, Event_Type = trs_et_, Reason_Code2 = trs_rc2_ , Qty = Qty * trs_qty_multiplier_,
   			    Gross_Value = Gross_Value * trs_qty_multiplier_, 
        	    Net_Value   = Net_Value   * trs_qty_multiplier_,
        	    Vat_Value   = Vat_Value   * trs_qty_multiplier_,
        	    Discount    = Discount    * trs_qty_multiplier_
               WHERE CURRENT OF cursor_tmr_ssal;
            
            END IF;
 
         EXCEPTION
            WHEN NO_DATA_FOUND THEN -- continue 
               Event_Log.Log_Event('No Translation for: ' || rec_tmr_ssal.Document_Type || rec_tmr_ssal.Owner || rec_tmr_ssal.Reason_Code ,MODULE_NM_,'W');      
            WHEN OTHERS THEN 
               RAISE;
            
         END; -- END autonomous block

   END LOOP;
 
   RETURN 0;

EXCEPTION
   WHEN OTHERS THEN RETURN -1;

END Extract_Street_Sales;


/* -------------------------------------------------------------------------- 
Extract_Consignment_Sales
-------------------------------------------------------------------------- */
FUNCTION Extract_Consignment_Sales RETURN BINARY_INTEGER IS

   CURSOR cursor_tmr_csal IS
   SELECT * FROM Ans_Sad_Owner.Tmr_Consignment_Sales FOR UPDATE;
   
   trs_ec_               Ans_Sad_Owner.Tmr_Translation.EVENT_CODE%TYPE;
   trs_et_               Ans_Sad_Owner.Tmr_Translation.EVENT_TYPE%TYPE;
   trs_rc2_              Ans_Sad_Owner.Tmr_Translation.REASON_CODE2%TYPE;
   trs_qty_multiplier_   Ans_Sad_Owner.Tmr_Translation.Csales_Qty%TYPE;

BEGIN
   MODULE_NM_ := 'TMR_ECSAL';
   
   DELETE FROM Ans_Sad_Owner.Tmr_Consignment_Sales;  -- TRUNCATE would be better
   
   INSERT INTO Ans_Sad_Owner.Tmr_Consignment_Sales
   SELECT /*+ INDEX_COMBINE(TMR_TAMRO) */  
    -- Columns common with Tmr_Tamro
    START_TAG,
    TRANSACTION_DATE,
    PRINCIPAL_NUMBER,
    ITEM_NUMBER,
    NORDIC_ITEM_NUMBER,
    PRINCIPAL_ITEM_NUMBER ,
    EAN_CODE,
    CUSTOMER_NUMBER_1,
    GEOREG_1,
    SUBGEOREG_1,
    COUNTRY_1,
    CUSTOMER_NUMBER_2,
    GEOREG_2,
    SUBGEOREG_2,
    COUNTRY_2,
    WAREHOUSE,
    TRANSACTION_TYPE,
    SIMPLIFIED_DOCUMENT_TYPE,
    DOCUMENT_TYPE ,
    REASON_CODE ,
    RETURN_REASON  ,
    ORDER_DATE   ,
    ORDER_NUMBER ,
    ORDER_LINE ,
    CUSTOMER_ORDER_NUMBER  ,
    PRINCIPALS_ORDER_NUMBER ,
    CURRENCY ,
    OWNER  ,
    LOT  ,
    EXPIRY_DATE  ,
    LOT_STATUS ,
    QTY  ,
    BACKORDER_DATE ,
    GROSS_VALUE ,
    DISCOUNT,
    NET_VALUE,
    VAT_VALUE  ,
    VAT_PERCENTAGE ,
    LIST_PRICE_PURCHASE ,
    LIST_PRICE_SALES,
    PRODUCT_CLASS,
    -- Extra fields
    NULL,
    NULL,
    NULL
   FROM Ans_Flat_Owner.Tmr_Tamro
   WHERE
     (
     DOCUMENT_TYPE IN ('T1','TU')
     AND OWNER = 'P'
     AND CUSTOMER_NUMBER_1 IN (3384, 3385, 3386, 3387)
     )
     OR
     (
     DOCUMENT_TYPE IN ('T1','T8')
     AND OWNER = 'T'
     AND CUSTOMER_NUMBER_1 IN (3389, 3390, 3391)
     )
     OR
     (
     DOCUMENT_TYPE = 'T9'
     AND OWNER = 'T'
     )
     OR
     (
     DOCUMENT_TYPE = 'I1'
     AND OWNER = 'P'
     AND REASON_CODE LIKE 'W%'
     )
     OR
     (
     DOCUMENT_TYPE = 'CR'
     AND OWNER = 'P'
     AND REASON_CODE IN ('R02','R03','R04','R05','R11')
     )
     OR
     (
     DOCUMENT_TYPE IN ('IF','S8','SA','SO','SD','SG','CO','PI','IM','TA')
     AND OWNER = 'P'
     )
     OR
     ( REASON_CODE = 'I21' );  

       
   -- Update according to specification
   FOR rec_tmr_csal IN cursor_tmr_csal -- all records
   LOOP
         BEGIN -- BEGIN autonomous block to handle in-loop exceptions
         
            IF rec_tmr_csal.Reason_Code IS NULL THEN
            
    		   SELECT B.Event_Code, B.Event_Type, B.Reason_Code2, B.Csales_Qty
               INTO trs_ec_, trs_et_, trs_rc2_, trs_qty_multiplier_
               FROM Ans_Sad_Owner.Tmr_Translation B
               WHERE B.Doc_Type  = rec_tmr_csal.Document_Type
                 AND B.Owner = rec_tmr_csal.Owner
                 AND B.Reason_Code IS NULL
                 AND ROWNUM = 1;
                 
               UPDATE Ans_Sad_Owner.Tmr_Consignment_Sales
               SET Event_Code = trs_ec_, Event_Type = trs_et_, Reason_Code2 = trs_rc2_ , Qty = Qty * trs_qty_multiplier_,
   			    Gross_Value = Gross_Value * trs_qty_multiplier_, 
        	    Net_Value   = Net_Value   * trs_qty_multiplier_,
        	    Vat_Value   = Vat_Value   * trs_qty_multiplier_,
        	    Discount    = Discount    * trs_qty_multiplier_
               WHERE CURRENT OF cursor_tmr_csal;
                
            ELSE 
            
    		   SELECT B.Event_Code, B.Event_Type, B.Reason_Code2, B.Csales_Qty
               INTO trs_ec_, trs_et_, trs_rc2_, trs_qty_multiplier_
               FROM Ans_Sad_Owner.Tmr_Translation B
               WHERE B.Doc_Type  = rec_tmr_csal.Document_Type
                 AND B.Owner = rec_tmr_csal.Owner
                 AND B.Reason_Code = rec_tmr_csal.Reason_Code
                 AND ROWNUM = 1;
                 
               UPDATE Ans_Sad_Owner.Tmr_Consignment_Sales
               SET Event_Code = trs_ec_, Event_Type = trs_et_, Reason_Code2 = trs_rc2_ , Qty = Qty * trs_qty_multiplier_,
  			    Gross_Value = Gross_Value * trs_qty_multiplier_, 
        	    Net_Value   = Net_Value   * trs_qty_multiplier_,
        	    Vat_Value   = Vat_Value   * trs_qty_multiplier_,
        	    Discount    = Discount    * trs_qty_multiplier_
               WHERE CURRENT OF cursor_tmr_csal;
            
            END IF;
            
         EXCEPTION
            WHEN NO_DATA_FOUND THEN -- continue 
               Event_Log.Log_Event('No Translation for: ' || rec_tmr_csal.Document_Type || rec_tmr_csal.Owner || rec_tmr_csal.Reason_Code ,MODULE_NM_,'W');      
            WHEN OTHERS THEN 
               RAISE;
            
         END; -- END autonomous block
          
   END LOOP;

   RETURN 0;

EXCEPTION
   WHEN OTHERS THEN RETURN -1;

END Extract_Consignment_Sales;



/*************************************************************************
                   PUBLIC PROCEDURE LOAD_STEWARD_SCREENS
**************************************************************************/   
PROCEDURE Load_Steward_Screens IS 

step_               CHAR(1);
edm_org_id_         NUMBER;
geo_id_             NUMBER;
cust_name_          Ans_Sad_Owner.Tmrs_Mainscreen.Tmr_Name%TYPE;

CURSOR c_mainscreen IS
   SELECT * FROM Ans_Sad_Owner.Tmrs_Mainscreen FOR UPDATE;

BEGIN
   
   -- Set global variables
   SELECT USER,    SYSDATE, 'STW',    0,          'TMR_SWS'
   INTO   LD_USR_, LD_DT_,  FLOW_NM_, PRG_ERRORS_, MODULE_NM_
   FROM Dual;
   
   -- Start Log and Get Batch ID
   Event_Log.Log_Start(MODULE_NM_);
   Pck_Btch_Stts.Pr_Insr_Batch('F',FLOW_NM_, SRC_SYS_,'RU');
   LD_ID_ := Pck_Btch_Stts.Fct_Extract_Batch('F',FLOW_NM_, SRC_SYS_);
   Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_, SRC_SYS_,'RU',NULL,NULL,NULL);
   
   DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> Start '|| MODULE_NM_ || '.Batch ID is: ' || TO_CHAR(LD_ID_));

   step_ := '1';
-- Step 1. Insert into TMRS_MAINSCREEN

   DELETE ans_sad_owner.tmrs_mainscreen;
   INSERT INTO ANS_SAD_OWNER.TMRS_MAINSCREEN 
   (
      SELECT X.xref_source_key, C.name, C.Address, C.City, C.Zip_Code, LPAD(C.Region_Id, 3, '0'), NULL, NULL, 'N' -- 6.1
      FROM ans_sad_owner.xrf_xreference X, ans_sad_owner.tmrs_customer_sirs C
      WHERE X.xref_source_system = 'TAMRO' AND X.hold_flag = 'H'  AND X.xref_answers_table = 'EDM_ORG'
      AND   LPAD(X.xref_source_key, 8, '0') = C.sirs_tamro_id(+)
   );

   step_ := '2';
-- Step 2. Get default values for EDM_ORG_ID

   FOR r_mainscreen IN c_mainscreen 
   LOOP 
      edm_org_id_ := NULL;
      
      -- Get edm_gpltcl_area_id
      BEGIN
         SELECT edm_gpltcl_area_id INTO geo_id_ 
         FROM edm_gpltcl_area 
         WHERE edm_geo_pltcl_area_type_cd = 'BRICK' AND brick_nbr = r_mainscreen.TMR_SUBREG;
      EXCEPTION
         WHEN OTHERS THEN GOTO next_record; 
      END;
      

      cust_name_ := TRIM(REPLACE(r_mainscreen.Tmr_Name, 'APOTEKET','')); 
      
      -- Try to find a match in EDM_ORG
      BEGIN
         SELECT A.edm_org_id INTO edm_org_id_  
         FROM edm_org A, edm_physcl_adrs_org B 
         WHERE B.edm_org_id = A.edm_org_id 
         AND B.Adrs_Typ_Cd IN ('VISIT', 'MAIN') 
         AND B.pstl_cd = r_mainscreen.TMR_POSTNR  
         AND A.edm_gpltcl_area_id_brick = geo_id_
         AND UPPER(A.acrnym_nm) = UPPER(cust_name_);   
      EXCEPTION
         WHEN NO_DATA_FOUND THEN GOTO next_record;
         WHEN TOO_MANY_ROWS THEN GOTO next_record; -- Better: try find using Bsns_Nm instead of Acrnym_Nm
      END;
      
      UPDATE Ans_Sad_Owner.Tmrs_Mainscreen 
      SET Tmr_Edm_Org_Id = edm_org_id_
      WHERE CURRENT OF c_mainscreen;
   
<<next_record>>
       NULL;
       
   END LOOP;
   
   COMMIT;
   
   -- End Log and Batch 
   IF PRG_ERRORS_ = 0 THEN
      MODULE_NM_ := 'TMR_SWS';
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'OK');
      Pck_Btch_Stts.Pr_Stts(LD_ID_, 'F',FLOW_NM_,SRC_SYS_,'OK',NULL,NULL,NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> OK End of ' || MODULE_NM_ || ' (batch job: ' || TO_CHAR(LD_ID_) || ').' );
   ELSE
      RAISE PROGRAM_ERROR;
   END IF; 
   

EXCEPTION
   WHEN OTHERS THEN
      MODULE_NM_ := 'TMR_SWS#' || step_;
      Event_Log.Log_Event('Load Screens Data Failed' || SQLERRM,MODULE_NM_,'E');
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'KO');
      Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_,SRC_SYS_,'KO',SQLCODE,SUBSTR(SQLERRM,1,200),NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> KO End with ' || PRG_ERRORS_ || ' errors.');
      RAISE;
   
END Load_Steward_Screens;



/*************************************************************************
                     PUBLIC PROCEDURE LOAD_LOTS
**************************************************************************/   
PROCEDURE Load_Lots IS 

sts_      BINARY_INTEGER;

BEGIN
   
   -- Set global variables
   SELECT USER,    SYSDATE, 'LOT',    0,          'TMR_LL'
   INTO   LD_USR_, LD_DT_,  FLOW_NM_, PRG_ERRORS_, MODULE_NM_
   FROM Dual;
   
   -- Start Log and Get Batch ID
   Event_Log.Log_Start(MODULE_NM_);
   Pck_Btch_Stts.Pr_Insr_Batch('F',FLOW_NM_, SRC_SYS_,'RU');
   LD_ID_ := Pck_Btch_Stts.Fct_Extract_Batch('F',FLOW_NM_, SRC_SYS_);
   Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_, SRC_SYS_,'RU',NULL,NULL,NULL);
   
   DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> Start '|| MODULE_NM_ || '.Batch ID is: ' || TO_CHAR(LD_ID_));

   -- Remove invalid records from FLAT table
   IF INVALID_REMOVED_ = TRUE THEN
      NULL;
   ELSE
      IF Remove_Invalid_Flat_Records() = 0 THEN
         COMMIT;
         Event_Log.Log_Event('Removed invalid flat records.','TMR_LL','I');
      ELSE
         ROLLBACK;
         Event_Log.Log_Event('Removal of invalid flat records FAILED','TMR_LL','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
         RAISE PROGRAM_ERROR; -- No point in proceeding
      END IF;
   END IF;
      
   /* Load lot data into EDM_LOT */
   IF Transform_Lot() = 0 THEN
      COMMIT; 
      Event_Log.Log_Event('Lot data loaded.','TMR_LL','I');
   ELSE
      ROLLBACK;     
      Event_Log.Log_Event('Lot data load failed.','TMR_LL','E');
      PRG_ERRORS_ := PRG_ERRORS_ + 1;
   END IF;

   -- End Log and Batch 
   IF PRG_ERRORS_ = 0 THEN
      MODULE_NM_ := 'TMR_LL';
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'OK');
      Pck_Btch_Stts.Pr_Stts(LD_ID_, 'F',FLOW_NM_,SRC_SYS_,'OK',NULL,NULL,NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> OK End of ' || MODULE_NM_ || ' (batch job: ' || TO_CHAR(LD_ID_) || ').' );
   ELSE
      RAISE PROGRAM_ERROR;
   END IF; 

EXCEPTION
   WHEN OTHERS THEN
      MODULE_NM_ := 'TMR_LL';
      Event_Log.Log_Event('Lot load failed -' || SQLERRM,MODULE_NM_,'E');
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'KO');
      Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_,SRC_SYS_,'KO',SQLCODE,SUBSTR(SQLERRM,1,200),NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> KO End with ' || PRG_ERRORS_ || ' errors.');
      RAISE;
   
END Load_Lots;


/*************************************************************************
                  PUBLIC PROCEDURE LOAD_CONSIG_SALES
**************************************************************************/   
PROCEDURE Load_Consig_Sales IS 

sts_      BINARY_INTEGER;

BEGIN
   
   -- Set global variables
   SELECT USER,    SYSDATE, 'CSAL',   0,          'TMR_LCS'
   INTO   LD_USR_, LD_DT_,  FLOW_NM_, PRG_ERRORS_, MODULE_NM_
   FROM Dual;
   
   -- Start Log and Get Batch ID
   Event_Log.Log_Start(MODULE_NM_);
   Pck_Btch_Stts.Pr_Insr_Batch('F',FLOW_NM_, SRC_SYS_,'RU');
   LD_ID_ := Pck_Btch_Stts.Fct_Extract_Batch('F',FLOW_NM_, SRC_SYS_);
   Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_, SRC_SYS_,'RU',NULL,NULL,NULL);
   
   DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> Start '|| MODULE_NM_ || '.Batch ID is: ' || TO_CHAR(LD_ID_));

   -- Remove invalid records from FLAT table
   IF INVALID_REMOVED_ = TRUE THEN
      NULL;
   ELSE
      IF Remove_Invalid_Flat_Records() = 0 THEN
         COMMIT;
         Event_Log.Log_Event('Removed invalid flat records.','TMR_LCS','I');
      ELSE
         ROLLBACK;
         Event_Log.Log_Event('Removal of invalid flat records FAILED','TMR_LCS','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
         RAISE PROGRAM_ERROR; -- No point in proceeding
      END IF;
   END IF;
   
   -- Transformation and Load
   sts_ := Extract_Consignment_Sales();
   IF  sts_ = 0 THEN -- From FLAT to SAD. 
      COMMIT;
      Event_Log.Log_Event('Consignment Sales Extracted.','TMR_LCS','I');
      sts_ := Transform_Consignment_Sales();
      IF  sts_ = 0 THEN
         COMMIT;
         Event_Log.Log_Event('Consignment Sales Loaded.','TMR_LCS','I');
      ELSE
         ROLLBACK;  
         Event_Log.Log_Event('Consignment Sales Load Failed.','TMR_LCS','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
      END IF;
   ELSE
      ROLLBACK;
      Event_Log.Log_Event('Consignment Sales Extract Failed.','TMR_LCS','E');
      PRG_ERRORS_ := PRG_ERRORS_ + 1;
   END IF;
  
   
   -- End Log and Batch 
   IF PRG_ERRORS_ = 0 THEN
      MODULE_NM_ := 'TMR_LCS';
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'OK');
      Pck_Btch_Stts.Pr_Stts(LD_ID_, 'F',FLOW_NM_,SRC_SYS_,'OK',NULL,NULL,NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> OK End of ' || MODULE_NM_ || ' (batch job: ' || TO_CHAR(LD_ID_) || ').' );
   ELSE
      RAISE PROGRAM_ERROR;
   END IF; 
   

EXCEPTION
   WHEN OTHERS THEN
      MODULE_NM_ := 'TMR_LCS';
      Event_Log.Log_Event('Consignment Sales Load Failed -' || SQLERRM,MODULE_NM_,'E');
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'KO');
      Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_,SRC_SYS_,'KO',SQLCODE,SUBSTR(SQLERRM,1,200),NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> KO End with ' || PRG_ERRORS_ || ' errors.');
      RAISE;

END Load_Consig_Sales;



/*************************************************************************
                  PUBLIC PROCEDURE LOAD_CONSIG_STOCK
**************************************************************************/   
PROCEDURE Load_Consig_Stocks IS 

sts_      BINARY_INTEGER;

BEGIN
   
   -- Set global variables
   SELECT USER,    SYSDATE, 'CSTK',   0,          'TMR_LCSK'
   INTO   LD_USR_, LD_DT_,  FLOW_NM_, PRG_ERRORS_, MODULE_NM_
   FROM Dual;
   
   -- Start Log and Get Batch ID
   Event_Log.Log_Start(MODULE_NM_);
   Pck_Btch_Stts.Pr_Insr_Batch('F',FLOW_NM_, SRC_SYS_,'RU');
   LD_ID_ := Pck_Btch_Stts.Fct_Extract_Batch('F',FLOW_NM_, SRC_SYS_);
   Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_, SRC_SYS_,'RU',NULL,NULL,NULL);
   
   DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> Start '|| MODULE_NM_ || '.Batch ID is: ' || TO_CHAR(LD_ID_));

   -- Remove invalid records from FLAT table
   IF INVALID_REMOVED_ = TRUE THEN
      NULL;
   ELSE
      IF Remove_Invalid_Flat_Records() = 0 THEN
         COMMIT;
         Event_Log.Log_Event('Removed invalid flat records.','TMR_LCSK','I');
      ELSE
         ROLLBACK;
         Event_Log.Log_Event('Removal of invalid flat records FAILED','TMR_LCSK','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
         RAISE PROGRAM_ERROR; -- No point in proceeding
      END IF;
   END IF;
   
   -- Transformation and Load  
   sts_  := Extract_Consignment_Stock();
   IF sts_ =  0 THEN -- From FLAT to SAD. 
      COMMIT;
      Event_Log.Log_Event('Consignment Stock Extracted.','TMR_LCSK','I');
      sts_ := Transform_Consignment_Stock();
      IF  sts_ = 0 THEN
         COMMIT;  
         Event_Log.Log_Event('Consignment Stock Loaded.','TMR_LCSK','I');
      ELSE
         ROLLBACK;
         Event_Log.Log_Event('Consignment Stock Load Failed.','TMR_LCSK','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
      END IF;
   ELSE
      ROLLBACK;
      Event_Log.Log_Event('Consignment Stock Extract Failed.','TMR_LCSK','E');
      PRG_ERRORS_ := PRG_ERRORS_ + 1;
   END IF;

   -- End Log and Batch 
   IF PRG_ERRORS_ = 0 THEN
      MODULE_NM_ := 'TMR_LCSK';
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'OK');
      Pck_Btch_Stts.Pr_Stts(LD_ID_, 'F',FLOW_NM_,SRC_SYS_,'OK',NULL,NULL,NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> OK End of ' || MODULE_NM_ || ' (batch job: ' || TO_CHAR(LD_ID_) || ').' );
   ELSE
      RAISE PROGRAM_ERROR;
   END IF; 
   

EXCEPTION
   WHEN OTHERS THEN
      MODULE_NM_ := 'TMR_LCSK';
      Event_Log.Log_Event('Consignment Stock Load Failed -' || SQLERRM,MODULE_NM_,'E');
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'KO');
      Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_,SRC_SYS_,'KO',SQLCODE,SUBSTR(SQLERRM,1,200),NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> KO End with ' || PRG_ERRORS_ || ' errors.');
      RAISE;
   
END Load_Consig_Stocks;
   
      
/*************************************************************************
                  PUBLIC PROCEDURE LOAD_STREET_SALES
**************************************************************************/   
PROCEDURE Load_Street_Sales (reload_after_manual_xrf_ IN VARCHAR2 DEFAULT 'first') 
IS 
sts_      BINARY_INTEGER;

BEGIN
   
   -- Set global variables
   SELECT USER,    SYSDATE, 'SSAL',   0,          'TMR_LSS'
   INTO   LD_USR_, LD_DT_,  FLOW_NM_, PRG_ERRORS_, MODULE_NM_
   FROM Dual;
   
   -- Start Log and Get Batch ID
   Event_Log.Log_Start(MODULE_NM_);
   Pck_Btch_Stts.Pr_Insr_Batch('F',FLOW_NM_, SRC_SYS_,'RU');
   LD_ID_ := Pck_Btch_Stts.Fct_Extract_Batch('F',FLOW_NM_, SRC_SYS_);
   Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_, SRC_SYS_,'RU',NULL,NULL,NULL);
   
   DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> Start '|| MODULE_NM_ || '.Batch ID is: ' || TO_CHAR(LD_ID_));

   -- Remove invalid records from FLAT table
   IF INVALID_REMOVED_ = TRUE THEN
      NULL;
   ELSE
      IF Remove_Invalid_Flat_Records() = 0 THEN
         COMMIT;
         Event_Log.Log_Event('Removed invalid flat records.','TMR_LSS','I');
      ELSE
         ROLLBACK;
         Event_Log.Log_Event('Removal of invalid flat records FAILED','TMR_LSS','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
         RAISE PROGRAM_ERROR; -- No point in proceeding
      END IF;
   END IF;
   
   -- Transformation and Load    
   IF reload_after_manual_xrf_ = 'AFTER_MXRF' THEN 
    
      sts_ := Transform_Street_Sales();
      IF  sts_ = 0 THEN
         -- COMMIT; V14 - commit is done inside the function
         Event_Log.Log_Event('Street Sales Loaded.','TMR_LSS','I');
      ELSE
         ROLLBACK;    
         Event_Log.Log_Event('Street Sales Load Failed.','TMR_LSS','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
      END IF;

   ELSE -- if it is not a reload after manual xrf
    
      sts_ := Extract_Street_Sales();
      IF sts_ = 0 THEN -- From FLAT to SAD. 
         COMMIT;
         Event_Log.Log_Event('Street Sales Extracted.','TMR_LSS','I');
         sts_ := Transform_Street_Sales();
         IF  sts_ = 0 THEN
            -- COMMIT; V14 - commit is done inside the function
            Event_Log.Log_Event('Street Sales Loaded.','TMR_LSS','I');
         ELSE
            ROLLBACK;    
            Event_Log.Log_Event('Street Sales Load Failed.','TMR_LSS','E');
            PRG_ERRORS_ := PRG_ERRORS_ + 1;
         END IF;
      ELSE
         ROLLBACK;
         Event_Log.Log_Event('Street Sales Extract Failed.','TMR_LSS','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
      END IF;
	     
   END IF;
  
   -- End Log and Batch 
   IF PRG_ERRORS_ = 0 THEN
      MODULE_NM_ := 'TMR_LSS';
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'OK');
      Pck_Btch_Stts.Pr_Stts(LD_ID_, 'F',FLOW_NM_,SRC_SYS_,'OK',NULL,NULL,NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> OK End of ' || MODULE_NM_ || ' (batch job: ' || TO_CHAR(LD_ID_) || ').' );
   ELSE
      RAISE PROGRAM_ERROR;
   END IF; 
   

EXCEPTION
   WHEN OTHERS THEN
      MODULE_NM_ := 'TMR_LSS';
      Event_Log.Log_Event('Street Sales Load Failed -' || SQLERRM,MODULE_NM_,'E');
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'KO');
      Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_,SRC_SYS_,'KO',SQLCODE,SUBSTR(SQLERRM,1,200),NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> KO End with ' || PRG_ERRORS_ || ' errors.');
      RAISE;

END Load_Street_Sales;


/*************************************************************************
                     PUBLIC PROCEDURE LOAD_CIFPRICES
**************************************************************************/   
PROCEDURE Load_CifPrices IS 

sts_      BINARY_INTEGER;

BEGIN
   
   -- Set global variables
   SELECT USER,    SYSDATE, 'CIFP',    0,          'TMR_LCIF'
   INTO   LD_USR_, LD_DT_,  FLOW_NM_, PRG_ERRORS_, MODULE_NM_
   FROM Dual;
   
   -- Start Log and Get Batch ID
   Event_Log.Log_Start(MODULE_NM_);
   Pck_Btch_Stts.Pr_Insr_Batch('F',FLOW_NM_, SRC_SYS_,'RU');
   LD_ID_ := Pck_Btch_Stts.Fct_Extract_Batch('F',FLOW_NM_, SRC_SYS_);
   Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_, SRC_SYS_,'RU',NULL,NULL,NULL);
   
   DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> Start '|| MODULE_NM_ || '.Batch ID is: ' || TO_CHAR(LD_ID_));

   -- Remove invalid records from FLAT table
   IF INVALID_REMOVED_ = TRUE THEN
      NULL;
   ELSE
      IF Remove_Invalid_Flat_Records() = 0 THEN
         COMMIT;
         Event_Log.Log_Event('Removed invalid flat records.','TMR_LCIF','I');
      ELSE
         ROLLBACK;
         Event_Log.Log_Event('Removal of invalid flat records FAILED','TMR_LCIF','E');
         PRG_ERRORS_ := PRG_ERRORS_ + 1;
         RAISE PROGRAM_ERROR; -- No point in proceeding
      END IF;
   END IF;
      
   /* Load CIF prices data into EDM_LOT */
   IF Transform_CifPrices() = 0 THEN
      COMMIT;  
      Event_Log.Log_Event('CIF prices data loaded.','TMR_LCIF','I');
   ELSE
      ROLLBACK;     
      Event_Log.Log_Event('CIF prices data load failed.','TMR_LCIF','E');
      PRG_ERRORS_ := PRG_ERRORS_ + 1;
   END IF;

   -- End Log and Batch 
   IF PRG_ERRORS_ = 0 THEN
      MODULE_NM_ := 'TMR_LCIF';
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'OK');
      Pck_Btch_Stts.Pr_Stts(LD_ID_, 'F',FLOW_NM_,SRC_SYS_,'OK',NULL,NULL,NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> OK End of ' || MODULE_NM_ || ' (batch job: ' || TO_CHAR(LD_ID_) || ').' );
   ELSE
      RAISE PROGRAM_ERROR;
   END IF; 

EXCEPTION
   WHEN OTHERS THEN
      MODULE_NM_ := 'TMR_LCIF';
      Event_Log.Log_Event('CIF prices load failed -' || SQLERRM,MODULE_NM_,'E');
      Pck_Btch_Stts.Pr_Updt_Batch(LD_ID_,'KO');
      Pck_Btch_Stts.Pr_Stts(LD_ID_,'F',FLOW_NM_,SRC_SYS_,'KO',SQLCODE,SUBSTR(SQLERRM,1,200),NULL);
      Event_Log.Log_Finish(MODULE_NM_);
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> KO End with ' || PRG_ERRORS_ || ' errors.');
      RAISE;
   
END Load_CifPrices;


/*************************************************************************
                     PUBLIC PROCEDURE Unload
**************************************************************************/
PROCEDURE Unload (exclude_ssal_ IN VARCHAR2 DEFAULT 'NO')  IS 

BEGIN
   MODULE_NM_ := 'TMR_U'; 
   DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> Start '|| MODULE_NM_ );

   DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_OUTGNG_SHPMNT_LOG';
   DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_OUTGNG_SHPMNT_LINE_LOG';
   DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_CUST_ORDR_LOG';
   DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_CUST_ORDR_LINE_LOG';
   DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_LOT_LOG';
   DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_EXT_INVNTRY_MVMNT_LOG';
   DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_INVNTRY_LOT_PRTN_LOG';
   DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_INVNTRY_NON_LOT_LOG';
   DELETE FROM ans_sad_owner.log_msg WHERE log_record_table = 'TMR_TAMRO_LOG';
   
   DELETE FROM ans_sad_owner.tmr_outgng_shpmnt_log;
   DELETE FROM ans_sad_owner.tmr_outgng_shpmnt_line_log;
   DELETE FROM ans_sad_owner.tmr_cust_ordr_log;
   DELETE FROM ans_sad_owner.tmr_cust_ordr_line_log;
   DELETE FROM ans_sad_owner.tmr_lot_log;
   -- DELETE FROM ans_sad_owner.tmr_ext_invntry_mvmnt_log;
   -- DELETE FROM ans_sad_owner.tmr_invntry_lot_prtn_log;
   DELETE FROM ans_sad_owner.tmr_invntry_non_lot_log;  
   -- DELETE FROM ans_sad_owner.tmr_tamro_log;
  
   IF exclude_ssal_ = 'NO' THEN
      DELETE FROM ANS_SAD_OWNER.LOG_MSG WHERE LOG_RECORD_TABLE = 'TMR_ACQRD_SALES_DATA_LOG'; 
      DELETE FROM ans_sad_owner.tmr_acqrd_sales_data_log;
   END IF;
   
   COMMIT;
   
   DELETE FROM ans_core_ext_owner.ext_invntry_mvmnt;
   DELETE FROM ans_edm_owner.edm_outgng_shpmnt_line WHERE edm_prmry_src_systm_cd = SRC_SYS_;
   DELETE FROM ans_edm_owner.edm_cust_ordr_line WHERE edm_prmry_src_systm_cd = SRC_SYS_;
   DELETE FROM ans_edm_owner.edm_outgng_shpmnt WHERE edm_prmry_src_systm_cd = SRC_SYS_;
   DELETE FROM ans_edm_owner.edm_cust_ordr WHERE edm_prmry_src_systm_cd = SRC_SYS_;
   DELETE FROM ans_edm_owner.edm_invntry_lot_prtn WHERE edm_prmry_src_systm_cd = SRC_SYS_;
   DELETE FROM ans_edm_owner.edm_invntry_non_lot WHERE edm_prmry_src_systm_cd = SRC_SYS_;     
   DELETE FROM ans_edm_owner.edm_lot WHERE edm_prmry_src_systm_cd = SRC_SYS_;
   DELETE FROM ans_edm_owner.edm_item_prc WHERE edm_prmry_src_systm_cd = SRC_SYS_;
   
   IF exclude_ssal_ = 'NO' THEN
      DELETE FROM ans_edm_owner.edm_acqrd_sales_data WHERE edm_prmry_src_systm_cd = SRC_SYS_;
	  COMMIT;
   END IF;
      
   IF exclude_ssal_ = 'NO' THEN
      DELETE FROM ans_sad_owner.xrf_xreference x WHERE x.xref_source_system = SRC_SYS_   
      AND x.XREF_LOG_TABLE IS NOT NULL; -- All Log entries
	  -- V15: delete manual customer-XRF entries
     DELETE FROM ans_sad_owner.xrf_xreference WHERE xref_source_system = SRC_SYS_ 
	 AND xref_answers_table = 'EDM_ORG' AND hold_flag = 'L';
   ELSE
      DELETE FROM ans_sad_owner.xrf_xreference x WHERE x.xref_source_system = SRC_SYS_   
      AND x.XREF_LOG_TABLE IS NOT NULL AND x.XREF_LOG_TABLE != 'TMR_ACQRD_SALES_DATA_LOG'; -- All Log entries but SSAL
   END IF;
   
   DELETE FROM ans_sad_owner.xrf_xreference x WHERE x.xref_source_system = SRC_SYS_   
   AND x.XREF_ANSWERS_TABLE = 'EDM_LOT'; -- Master LOT entries
   
   COMMIT; 
   
   DELETE FROM ans_sad_owner.ans_batch WHERE SRC_SYSTM_CD = SRC_SYS_;
   
   DELETE FROM system_events WHERE module_name LIKE 'TMR_%';
   
   COMMIT;
   
   DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> OK End of ' || MODULE_NM_  );

EXCEPTION
   WHEN OTHERS THEN
      ROLLBACK;
      MODULE_NM_ := 'TMR_U';
      Event_Log.Log_Event('Unload failed -' || SQLERRM,MODULE_NM_,'E');
      DBMS_Output.Put_Line( TO_CHAR(SYSDATE, 'DD-MM-YY HH24:MI:SS') || '> KO End of ' || MODULE_NM_ );
      RAISE;
   
END Unload;


END Pck_Tamro_Interface;
/
/*<TOAD_FILE_CHUNK>*/


GRANT EXECUTE ON Pck_Tamro_Interface TO ANS_LA5;
/
