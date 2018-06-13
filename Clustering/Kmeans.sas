data Cluster_prep_a ; set merge_2 ; 
    if vendor ne ' ' ;
    if Neg_Vol = 0 ;
    if High_Vol = 1 ; 
    if Delete_me = 0 ; 
    if vendor_parent_n  ne 'ECOVA' ;
    if VendCat1 ne 'DISTRIBUTION & TRANSPORTATION' ;
    array Zero _numeric_ ; 
     do over Zero ;
        if Zero = . then Zero =  0 ; 
    end ;
    check_sum1 = sum(AMOUNT_NNN ,  AMOUNT_NNY , AMOUNT_NYN ,  AMOUNT_YNN ,
                                         AMOUNT_YYN ,  AMOUNT_YNY , AMOUNT_NYY , AMOUNT_YYY )
                  ;
    check_sum2 = sum(
                                        AMOUNT_Pos_NNN , AMOUNT_Neg_NNN ,
                                        AMOUNT_Pos_NNY , AMOUNT_Neg_NNY ,  
                                        AMOUNT_Pos_NYN , AMOUNT_Neg_NYN , 
                                        AMOUNT_Pos_YNN , AMOUNT_Neg_YNN , 
                                        AMOUNT_Pos_YYN , AMOUNT_Neg_YYN , 
                                        AMOUNT_Pos_YNY , AMOUNT_Neg_YNY , 
                                        AMOUNT_Pos_NYY , AMOUNT_Neg_NYY , 
                                        AMOUNT_Pos_YYY , AMOUNT_Neg_YYY )
                  ;
    check_test = check_sum1 - Total_Spend_M40 ;
    check_test2 = check_sum2 - Total_Spend_M40 ;
    format amount_by_vendor dollar20. Total_Spend_M40 dollar20. Total_Spend_M40 dollar20. Total_Spend_Pos_M40 dollar20. 
                  Total_Spend_Neg_M40 dollar20. AMOUNT_NNN dollar20. AMOUNT_Pos_NNN dollar20. AMOUNT_Neg_NNN dollar20.
                  AMOUNT_NNY dollar20. AMOUNT_Pos_NNY dollar20. AMOUNT_NYN dollar20. AMOUNT_Pos_NYN dollar20. 
                  AMOUNT_Neg_NYN dollar20. AMOUNT_YNN dollar20. AMOUNT_Pos_YNN dollar20. AMOUNT_Neg_YNN dollar20. 
                  AMOUNT_YYN dollar20. AMOUNT_Pos_YYN dollar20. AMOUNT_YNY dollar20. AMOUNT_Pos_YNY dollar20. 
                  AMOUNT_Neg_YNY dollar20. AMOUNT_NYY dollar20. AMOUNT_Pos_NYY dollar20. AMOUNT_Neg_NYY dollar20. 
                  AMOUNT_YYY dollar20. AMOUNT_Pos_YYY dollar20. AMOUNT_Neg_YYY dollar20. 
                  check_sum1 dollar20. check_sum2 dollar20. check_test dollar20. check_test2 dollar20.
    ; 
run ;   
    /* check amount totals */ proc means data = Cluster_prep_a missing n sum mean std min max cv ; var check_test check_test2 ; run ; 
                                                      proc freq data = Cluster_prep_a ; tables Vendor ; run ;     /* 1608 */
                                                      proc sql; create table test as 
                                                        select distinct Vendor , vendor_parent_n  
                                                        from Cluster_prep_a 
                                                            where VendCat1 = 'DISTRIBUTION & TRANSPORTATION' /* remember that you have to add back in to run code */
                                                        order by vendor_parent_n 
                                                      ; quit ; 
                                                      proc univariate data = Cluster_prep_a plots normaltest ; var Perc_Ctrct_Spend ; run ;  
; /*** Prep data to obtain the Cluster Variables and put into macro ***/
    data Cluster_prep_b ; set Cluster_prep_a 
            (drop = check_sum1 check_sum2 check_test check_test2 Neg_Vol High_Vol Delete_Pyramids Delete_f Delete_me ) ; 
    run ;  
;   /*    NOTE: The data set WORK.CLUSTER_PREP has 1396 observations and 62 variables.*/
    data Cluster_prep ; set Cluster_prep_b
            (drop =  amount_vendor_parent amount_by_vendor 
                            Total_Spend_M40 Total_Spend_Pos_M40 Total_Spend_Neg_M40 Total_Ctrct_Spend
                            AMOUNT_NNN AMOUNT_NNY AMOUNT_NYN AMOUNT_YNN 
                            AMOUNT_YYN AMOUNT_YNY AMOUNT_NYY AMOUNT_YYY 
                            Perc_HQ_Spend Perc_Fld_Spend Perc_Ctrct_Spend Perc_HQ_Ctrct_Spend Perc_Fld_Ctrct_Spend Perc_Ariba_M40
                            VendCat1
            ) ; 
    run ;  
    proc means data = Cluster_prep  n sum mean std min max range ; output out = Var_sums ; run ;           
    proc transpose data = Var_sums
        out = Var_sums_trans (rename= (_name_=Var COL1=Obs COL2=Min COL3 = Max COL4 = Mean COL5 = Std )) ;
    run ; 
    %Global Var_Clusters ; 
    proc sql; select Var into: Var_Clusters separated by ' ' from Var_sums_trans where STD ne 0 and Var not in ( '_TYPE_','_FREQ_') ; quit ; 
    %put &Var_Clusters ; 
            proc means data = Cluster_prep  n sum mean std min max range ; var &Var_Clusters ; run ; 
     ;   /* NOTE: The data set WORK.CLUSTER_PREP has 1396 observations and 49 variables.*/
     data Cluster_prep ; set Cluster_prep ; keep Vendor &Var_Clusters ; run ; 
 
; /*** Begin clustering process with variables selected above ***/
proc standard mean=0 std=1 data=Cluster_prep out= Cluster_prep_std ; *Standardize the data*;
    var &Var_Clusters. ;  
run; proc means data = cluster_Prep_std ; run; 

proc sql; drop table Stats_summary ; quit ; 

;/* Macro to find k-clusters for k-means */ 
%macro wss_loop(n=) ;
%do i = 1 %to &n. ; 
ods trace on ;
ods output VariableStat = WSS (where = (variable = 'OVER-ALL' )) ;
ods output CCC = CCC ; 
ods output PseudoFStat = PseudoFStat ; 
ods listing close ; 
proc fastclus data=Cluster_prep_std  
                        maxc=&i. out= V_clusters random = 9999 replace = random ;
    var &Var_Clusters.  ;
run;
ods trace off ; 
ods listing ;

data WSS_cnt ; set WSS ; count = &i. ; run ; 
data CCC_cnt ; set CCC ; count = &i. ; run ; 
data PseudoFStat_cnt ; set PseudoFStat ; count = &i. ; run ; 

proc sql; create table Stats_merge as
    select a.* , b.value as CCC , c.value as Fstat 
    from WSS_cnt as a
    left join CCC_cnt as b
    on a.count = b.count
    left join PseudoFStat_cnt as c
    on a.count = c.count
; quit ; 

proc append base = Stats_summary data = Stats_merge force ; run ; 
 
%end ;
proc sql; select count into: Clust_no from Stats_summary having RSqRatio = max(RSqRatio) ; quit ;  /*RSquare, RSqRatio*/
%put "The number of clusters for the K-means analysis is %sysfunc(trim(&Clust_no.))"  ;

; /* Run the k-means with the best number of clusters */
ods trace on ; 
ods listing close ; 
ods output ClusterCenters = ClusterCenters ;  
proc fastclus data=Cluster_prep_std outseed = seed outstat = Stats
                        maxc=&Clust_no. out= V_clusters random = 9999 replace = random ;                            
    var &Var_Clusters. ;
run; 
ods trace off ; ods listing ; 
%mend wss_loop ; 
;/* set max number of clusters to test as n= */
%wss_loop(n=30) ;          

; /* TestCode : Run the k-means with the best number of clusters */
%macro Notes ; 
ods trace on ; ods listing close ; %put &Clust_no ;
ods output CCC = Tests ; 
ods output PseudoFStat = PseudoFStat ;
ods output ObsOverAllRSquare = ObsOverAllRSquare ; 
ods output ClusterCenters = ClusterCenters ;
proc fastclus data=Cluster_prep_std /*(drop = High_Vol Neg_vol amount_vendor_parent) */ maxc=23 out= V_clusters mean = Mean
                            random = 9999 replace = random ; * summary;
    var &Var_Clusters.   ;
run;
ods trace off ; ods listing ; 
%mend Notes ;                                  

;   /*** Join Cluster data to the original data ***/ 
data Cluster_prep_b ; set Cluster_prep_b ; rename vendor =  vend_ref_i ; run ;  
data ClusterCenters ; set ClusterCenters ; cluster_no = compress(trim(put(Cluster, Char5.)),' ','') ; run ; 

; /* Add Clusters results to main dataset */
proc sql; create table Category_Clusters as
    select a.* , compress(trim(put(b.Cluster, Char5.)),' ','') as cluster_no , b.Distance 
    from Cluster_prep_b as a
    inner join V_clusters as b
    on a.vend_ref_i = b.vendor 
    order by Cluster , amount_by_vendor desc 
; quit ;
data sasuser.Category_Clusters ; set Category_Clusters ; run ; 

; /* Add Clusters results to cluster centers dataset */
proc sql; create table Category_Cluster_Centers (drop = cluster) as
    select a.vend_ref_i , a.cluster_no , a.Distance , c.*
    from Category_Clusters as a
    inner join ClusterCenters as c 
    on a.cluster_no = c.cluster_no
; quit ;  
data sasuser.Category_Cluster_Centers ; set Category_Cluster_Centers ; run ; 

; /*** EDA the clustered data ***/
proc means data = Category_Clusters n sum mean std min max range missing ;* by cluster_no ; run ; 

/* Upload to Teradata */
proc sql; drop table T_SAPDEV.Category_Clusters ; quit ; 
data  T_SAPDEV.Category_Clusters  
             (Fastload = YES tpt = yes  
                dbtype=(vend_ref_i = 'VARCHAR(10)' cluster_no = 'Char(2)'  ) 
                dbcreate_table_opts = "primary index( vend_ref_i , vendor_parent_n , cluster_no )"
              ) ;
    set sasuser.Category_Clusters ;
run ; 

proc sql; drop table T_SAPDEV.Category_Cluster_Centers ; quit ; 
data T_SAPDEV.Category_Cluster_Centers  
         (Fastload = YES
                dbtype=(vend_ref_i = 'VARCHAR(10)' cluster_no = 'Char(2)'  ) 
                dbcreate_table_opts = "primary index( vend_ref_i , cluster_no )"
          ) ;           
set sasuser.Category_Cluster_Centers ; 
run ; 
