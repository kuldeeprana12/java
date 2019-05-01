CREATE OR REPLACE PACKAGE BODY OL00.pkg_ra_process_ELA
IS
-------------------------------------------------------------------------------------------------------------------------
   -- Declare Constants
   c_process_ixos_ra        CONSTANT CHAR (1)                       := 'X';
   c_process_sap_ra         CONSTANT CHAR (1)                       := 'R';
   c_unprocessed_ra         CONSTANT NUMBER                         := 0;
   c_processsed_ra_ack      CONSTANT NUMBER                         := 1;
   c_process_error_ra_ack   CONSTANT NUMBER                         := -1;
   c_unprocesssed_ixos_ack  CONSTANT NUMBER                         := -2;
   c_process_error_ixos_ack   CONSTANT NUMBER                         := -3;
   v_current_datetime                DATE;
   TYPE refcursor IS REF CURSOR;
   TYPE rec_all_unprocess_ra_acks_type IS RECORD (
      row_id            ROWID,
      sap_idoc_num      s_ra_wrk.sap_idoc_num%TYPE,
      cust_acct_id      s_ra_wrk.cust_acct_id%TYPE,
      cust_cr_id        s_cr.cust_cr_id%TYPE,
      cust_cr_uniq_id   s_ra_wrk.cust_cr_uniq_id%TYPE,
      crte_dt           s_ra_wrk.crte_dt%TYPE,
      ra_id             s_ra_wrk.ra_id%TYPE,
      ra_ixos_ind       s_ra_wrk.ra_ixos_ind%TYPE,
      rtn_po_num        s_ra_wrk.rtn_po_num%TYPE,
      ra_num            s_ra_wrk.ra_num%TYPE,
      ra_dt             s_ra_wrk.ra_dt%TYPE,
      ra_valid_to       s_ra_wrk.ra_valid_to%TYPE,
      spcl_hndl_cd      s_ra_wrk.spcl_hndl_cd%TYPE,
      ixos_archive_id   s_ra_wrk.ixos_archive_id%TYPE,
      pnt_flg           s_ra_wrk.pnt_flg%TYPE
   );
   TYPE ra_wrk_lin_rec_type IS RECORD (
      sap_idoc_num   s_ra_lin_wrk.sap_idoc_num%TYPE,
      item_num       s_ra_lin_wrk.item_num%TYPE,
      invc_num       s_ra_lin_wrk.invc_num%TYPE,
      invc_dt        s_ra_lin_wrk.invc_dt%TYPE,
      rtn_item_qty   s_ra_lin_wrk.rtn_item_qty%TYPE,
      rsn_cd         s_ra_lin_wrk.rsn_cd%TYPE,
      rtn_rjct_cd    s_ra_lin_wrk.rtn_rjct_cd%TYPE
   );
   rec_all_unprocess_ra_acks         rec_all_unprocess_ra_acks_type;
   TYPE ra_wrk_lin_type IS TABLE OF ra_wrk_lin_rec_type
      INDEX BY BINARY_INTEGER;
   ra_wrk_lin_tab                    ra_wrk_lin_type;
   FUNCTION open_ra_cursor (type_of_ra_ack CHAR, v_batch_size NUMBER)
      RETURN refcursor;
   PROCEDURE process_ra (type_ra CHAR);
   PROCEDURE process_credit (
      ra_wrk_lin_tab    IN       ra_wrk_lin_type,
      v_cr_id           OUT      s_cr.cr_id%TYPE
   );
   PROCEDURE process_ra_update (
      rec_unproc_acks   IN       rec_all_unprocess_ra_acks_type,
      v_cr_id           IN       S_cr.cr_id%TYPE,
      v_ra_id           OUT      s_ra.ra_id%TYPE,
      updt_ra_table     IN OUT   BOOLEAN
   );
   PROCEDURE process_ra_item (
      ra_wrk_lin_tab   IN   ra_wrk_lin_type,
      v_ra_id          IN   s_ra.ra_id%TYPE,
      crte_dt          IN   s_ra_item.crte_dt%TYPE,
      updt_ra_table    IN   BOOLEAN
   );
   PROCEDURE update_ra_for_ixos;
----------------------------------------------------------------------------------------------------------------------
   PROCEDURE process_ixosra
   IS
   BEGIN
      process_ra (c_process_ixos_ra);
   END;
   PROCEDURE process_sapra
   IS
   BEGIN
      process_ra (c_process_sap_ra);
   END;
   FUNCTION open_ra_cursor (type_of_ra_ack CHAR, v_batch_size NUMBER)
      RETURN refcursor
   IS
      cur_unprocessed_ra_acks   refcursor;
   BEGIN
      IF type_of_ra_ack = c_process_ixos_ra
      THEN
         OPEN cur_unprocessed_ra_acks FOR
            SELECT        rw.ROWID, rw.sap_idoc_num, substr(rw.cust_acct_id,5,6),
                          rw.cust_cr_id, rw.cust_cr_uniq_id, rw.crte_dt,
                          rw.ra_id, rw.ra_ixos_ind, rw.rtn_po_num,
                          rw.ra_num, nvl(rw.ra_dt,sysdate), rw.ra_valid_to,
                          rw.spcl_hndl_cd, rw.ixos_archive_id,pnt_flg
                     FROM s_ra_wrk rw
                    WHERE rw.ra_id IS NOT NULL
                      AND rw.ra_ixos_ind = c_process_ixos_ra
                      AND rw.ra_id = 0
		     AND CAST (TRIM (SUBSTR (cust_acct_id, 5, 6)) AS CHAR (10)) in (SELECT cust_acct_id FROM s_drug_cust_acct )
                      AND ROWNUM <= v_batch_size
					   order by crte_dt
		 -- In the above where caluse the cust_acct_id equality is added to filter non-smo accounts from getting processed.
            FOR UPDATE OF rw.ra_id SKIP LOCKED;
      ELSIF type_of_ra_ack = c_process_sap_ra
      THEN
         OPEN cur_unprocessed_ra_acks FOR
            SELECT        rw.ROWID, rw.sap_idoc_num, substr(rw.cust_acct_id,5,6),
                          rw.cust_cr_id, rw.cust_cr_uniq_id, rw.crte_dt,
                          rw.ra_id, rw.ra_ixos_ind, rw.rtn_po_num,
                          rw.ra_num,nvl(rw.ra_dt,sysdate), rw.ra_valid_to,
                          rw.spcl_hndl_cd, rw.ixos_archive_id,pnt_flg
                     FROM s_ra_wrk rw
                    WHERE rw.ra_id IS NOT NULL
                      AND rw.ra_ixos_ind = c_process_sap_ra
                      AND rw.ra_id = 0
		     AND CAST (TRIM (SUBSTR (cust_acct_id, 5, 6)) AS CHAR (10)) in (SELECT cust_acct_id FROM s_drug_cust_acct )
                      AND ROWNUM <= v_batch_size
					  order by crte_dt
		 -- In the above where caluse the cust_acct_id equality is added to filter non-smo accounts from getting processed.
            FOR UPDATE OF rw.ra_id SKIP LOCKED;
      END IF;
      RETURN (cur_unprocessed_ra_acks);
   END open_ra_cursor;
   PROCEDURE process_ra (type_ra CHAR)
   IS
      v_procobj                 fmwk.process_object;
      v_module_name    CONSTANT VARCHAR2 (60)
                                          := 'pkg_ra_process_ELA.process_ra';
      v_batch_size              NUMBER;
      TYPE refcursor IS REF CURSOR;
      cur_unprocessed_ra_acks   refcursor;
      v_ra_id                   s_ra.ra_id%TYPE;
      v_ra_item_id              s_ra_item.ra_item_id%TYPE;
      v_cr_id                   s_cr.cr_id%TYPE;
      v_processed_in_batch      INTEGER;
      v_rows_processed          INTEGER                     := 0;
      updt_ra_table             BOOLEAN                     := FALSE;
   BEGIN
      v_procobj    := fmwk.process_init(NAME => v_module_name);
      v_batch_size := fmwk.tx_size;
      BEGIN
         LOOP
            BEGIN
               fmwk.set_code_location ('1: Enter procedure');
               cur_unprocessed_ra_acks :=
                                       open_ra_cursor (type_ra, v_batch_size);
               -- Loop until no more records to process
               v_processed_in_batch := 0;
               LOOP
                  BEGIN
                     FETCH cur_unprocessed_ra_acks
                      INTO rec_all_unprocess_ra_acks;
                     EXIT WHEN cur_unprocessed_ra_acks%NOTFOUND;
                     --One Row from ack header processed
                     v_processed_in_batch := v_processed_in_batch + 1;
                     --
                     -- Establish a savepoint for this transaction, ie, this sap_idoc_num
                     --
                     SAVEPOINT tx_ack;
                        ra_wrk_lin_tab.DELETE;
                        v_current_datetime := SYSDATE;
                        SELECT rlw.sap_idoc_num,
                               rlw.item_num,
                               rlw.invc_num,
                               rlw.invc_dt,
                               rlw.rtn_item_qty,
                               rlw.rsn_cd,
                               rlw.rtn_rjct_cd
                        BULK COLLECT INTO ra_wrk_lin_tab
                          FROM s_ra_lin_wrk rlw
                         WHERE rlw.sap_idoc_num =
                                        rec_all_unprocess_ra_acks.sap_idoc_num;
			  IF type_ra = c_process_sap_ra
				then
                      if ra_wrk_lin_tab.count = 0 then
                      v_cust_text := 'No data in s_ra_lin_wrk';
                           fmwk.set_code_location
                              ('3: When checking for records at s_ra_lin_wrk');
                           RAISE ra_ack_error;
                     end if;
                   end if;
                     if rec_all_unprocess_ra_acks.ra_num is null
                     then
                            dbms_output.put_line('rec_all_unprocess_ra_acks.ra_num ' || rec_all_unprocess_ra_acks.ra_num);
                             v_cust_text := 'Ra Number is null';
                           fmwk.set_code_location
                              ('3: When checking if RA number is present');
                             RAISE ra_ack_error;
                      end if;
                     -- If processing IXOS RA then just update the IXOS Archive ID is S_RA table based on the RA number
                     -- If processing SAP RA then insert/update row in S_RA and S_RA_ITEM
                     IF type_ra = c_process_ixos_ra
                     THEN
                        update_ra_for_ixos;
                     ELSE
                        process_credit
                                  (ra_wrk_lin_tab,
                                   v_cr_id
                                  );
                        process_ra_update (rec_all_unprocess_ra_acks,
                                    v_cr_id,
                                    v_ra_id,
                                    updt_ra_table
                                   );
                        process_ra_item (ra_wrk_lin_tab,
                                         v_ra_id,
                                         rec_all_unprocess_ra_acks.crte_dt,
                                         updt_ra_table
                                        );
                        v_rows_processed := v_rows_processed + 1;
                         -- update RA Ack in work table with matched/inserted RA_ID
                         UPDATE s_ra_wrk rw
                            SET rw.ra_id = v_ra_id,
                                rw.ra_updt_dts = v_current_datetime
                          WHERE ROWID = rec_all_unprocess_ra_acks.row_id;
                     END IF;                                  -- If clause end
                     --update done so increase rows processed
                     v_rows_processed := v_rows_processed + 1;
                              --
                     -- Indicate end of transaction
                     -- If fmwk.tx_commit, returns TRUE, a physical commit was issued
                     -- If so, reopen the cursor (The commit would have released the row level locks obtained with the for update clause)
                     IF fmwk.tx_commit = TRUE
                     THEN
                        CLOSE cur_unprocessed_ra_acks;
                        cur_unprocessed_ra_acks :=
                                       open_ra_cursor (type_ra, v_batch_size);
                        v_processed_in_batch := 0;
                     END IF;
                  EXCEPTION
                     WHEN ra_ack_error
                     THEN
                        ROLLBACK TO SAVEPOINT tx_ack;
                        fmwk.log_error
                           (handle             => v_procobj,
                            custom_text        =>    'ra_ack_error '
                                                  || v_cust_text,
                            code_location      => fmwk.get_code_location (),
                            error_value        => rec_all_unprocess_ra_acks.sap_idoc_num
                           );
                        UPDATE s_ra_wrk rw
                           SET rw.ra_id = c_process_error_ra_ack,
                               rw.ra_updt_dts = v_current_datetime
                         WHERE ROWID = rec_all_unprocess_ra_acks.row_id;
                        v_rows_processed := v_rows_processed + 1;
                  END;
                  v_txns_processed := v_txns_processed + 1;
               END LOOP;
               CLOSE cur_unprocessed_ra_acks;
            END;
            EXIT WHEN v_processed_in_batch != v_batch_size;
         END LOOP;
         --
         fmwk.do_commit;
      END;
   END process_ra;
----------------------------------------------------------------------------------------------------------------------------
   PROCEDURE process_credit (
      ra_wrk_lin_tab    IN       ra_wrk_lin_type,
      v_cr_id           OUT      s_cr.cr_id%TYPE
   )
   IS
      l_sell_desc          s_drug_item.sell_dscr_txt%TYPE;
      l_mfg_unit_cd        s_drug_item.mfg_unit_cd%TYPE;
      --l_prc_cd             s_drug_item_prc.spcl_prc_cd%TYPE;
      --l_curr_ctlg_prc      s_drug_item_prc.proper_cntrc_prc%TYPE;
      l_unit_prc           s_drug_item.unit_sell_prc%TYPE;
      l_pri_cust_acct_id   s_drug_cust_acct.pri_cust_acct_id%TYPE;
      type num_tab is table of number;
      empno_tab            num_tab;
      v_count              number;
   BEGIN
      -- Create CR if it does not exist (insert into S_CR ).
      -- Return CR_ID.
      v_cr_id := 0;
      BEGIN
          SELECT cr.cr_id
                into v_cr_id
          FROM s_cr cr
              WHERE cr.cust_acct_id = rec_all_unprocess_ra_acks.cust_acct_id
                AND cr.cust_cr_id = rec_all_unprocess_ra_acks.cust_cr_id
                AND cust_cr_uniq_id = rec_all_unprocess_ra_acks.cust_cr_uniq_id
		AND ROWNUM=1;
      EXCEPTION
        WHEN no_data_found THEN
            v_cr_id := 0;
      END;
      IF (v_cr_id = 0) THEN
         INSERT INTO s_cr
                     (cr_id, cust_acct_id, cust_cr_id, cust_cr_uniq_id,
                      cr_stat_cd, updt_dts, user_id
                     )
              VALUES (s_cr#cr_id.NEXTVAL, rec_all_unprocess_ra_acks.cust_acct_id, rec_all_unprocess_ra_acks.cust_cr_id, rec_all_unprocess_ra_acks.cust_cr_uniq_id,
                      'S', v_current_datetime, 'EXTERNAL'
                     )
           RETURNING cr_id
                INTO v_cr_id;
         v_rows_processed := v_rows_processed + 1;
         SELECT NVL (pri_cust_acct_id, cust_acct_id)
           INTO l_pri_cust_acct_id
           FROM s_drug_cust_acct a
          WHERE a.cust_acct_id =CAST (TRIM ( cust_acct_id) AS CHAR (10)) AND ROWNUM = 1;
	-- exception handling part has been added for below select statement to filter out the mismatched items which are
	-- available in ra_lin_wrk table but not in drug_item table.
         FOR i IN ra_wrk_lin_tab.FIRST .. ra_wrk_lin_tab.LAST LOOP
            BEGIN
            SELECT sell_dscr_txt, mfg_unit_cd, unit_sell_prc INTO l_sell_desc, l_mfg_unit_cd, l_unit_prc FROM s_drug_item SD
            WHERE SD.item_num = ra_wrk_lin_tab (i).item_num AND ROWNUM = 1;
            EXCEPTION
            WHEN NO_DATA_FOUND
	    THEN
            l_sell_desc := 'N/A';
            l_mfg_unit_cd:= '1';
            l_unit_prc:= 0;
            END;
            --BEGIN
              -- SELECT spcl_prc_cd, proper_cntrc_prc
                -- INTO l_prc_cd, l_curr_ctlg_prc
                 --FROM s_drug_item_prc p
                --WHERE p.cust_acct_id = l_pri_cust_acct_id
                  --AND p.item_num = ra_wrk_lin_tab (i).item_num
                  --AND ROWNUM = 1;
           -- EXCEPTION
             --  WHEN NO_DATA_FOUND
               --THEN
                  --l_prc_cd := 9;
                  --l_curr_ctlg_prc := l_unit_prc;
            --END;
           -- INSERT INTO s_cr_item
           --             (cr_item_id, cr_id,
           --              item_num, sell_dscr_txt,
            --             rtn_item_qty, mfg_unit_cd,
            --             invc_num,
            --             invc_dt,
            --             rsn_cd, invc_prc, prc_cd,
            --             curr_ctlg_prc
            --            )
            --     VALUES (s_cr_item#cr_item_id.NEXTVAL, v_cr_id,
            --             ra_wrk_lin_tab (i).item_num, l_sell_desc,
            --             ra_wrk_lin_tab (i).rtn_item_qty, l_mfg_unit_cd,
            --             ra_wrk_lin_tab (i).invc_num,
            --             ra_wrk_lin_tab (i).invc_dt,
            --             substr(ra_wrk_lin_tab (i).rsn_cd,2,1), NULL, l_prc_cd,
            --             l_curr_ctlg_prc
            --            );
	    INSERT INTO s_cr_item
                        (cr_item_id, cr_id,
                         item_num, sell_dscr_txt,
                         rtn_item_qty, mfg_unit_cd,
                         invc_num,
                         invc_dt,
                         rsn_cd, invc_prc, prc_cd,curr_ctlg_prc
                        )
                VALUES (s_cr_item#cr_item_id.NEXTVAL, v_cr_id,
                         ra_wrk_lin_tab (i).item_num, l_sell_desc,
                         ra_wrk_lin_tab (i).rtn_item_qty, l_mfg_unit_cd,
                         ra_wrk_lin_tab (i).invc_num,
                         ra_wrk_lin_tab (i).invc_dt,
                        substr(ra_wrk_lin_tab (i).rsn_cd,2,1), NULL, NULL,NULL
                        );
            v_rows_processed := v_rows_processed + 1;
         END LOOP;
      END IF;
      dbms_output.put_line('process_credit: v_cr_id - ' || v_cr_id);
   END process_credit;
----------------------------------------------------------------------------------------------------------------------
   PROCEDURE process_ra_update (
      rec_unproc_acks   IN       rec_all_unprocess_ra_acks_type,
      v_cr_id           IN       S_cr.cr_id%TYPE,
      v_ra_id           OUT      s_ra.ra_id%TYPE,
      updt_ra_table     IN OUT   BOOLEAN
   )
   IS
   BEGIN
      -- For RAs resent from SAP, update S_RA first.
      -- If RA does not exist, then insert into S_RA table.
      -- Update boolean variable 'updt_ra_table'.
      UPDATE s_ra
         SET cr_id = v_cr_id,
             rtn_po_num = rec_all_unprocess_ra_acks.rtn_po_num,
             ra_dt = rec_all_unprocess_ra_acks.ra_dt,
             ra_valid_to = nvl(rec_all_unprocess_ra_acks.ra_valid_to,rec_all_unprocess_ra_acks.ra_dt + 30),
             spcl_hndl_cd = rec_all_unprocess_ra_acks.spcl_hndl_cd,
             RA_UPDT_DT = sysdate
       WHERE ra_num = rec_all_unprocess_ra_acks.ra_num
          RETURNING ra_id
                INTO v_ra_id;
      updt_ra_table := TRUE;
      v_rows_processed := v_rows_processed + 1;
      IF (SQL%ROWCOUNT = 0) THEN
         INSERT INTO s_ra
                     (ra_id,
                      cr_id,
                      cust_acct_id,
                      cust_cr_id,
                      cust_cr_uniq_id ,
                      crte_dt,
                      rtn_po_num,
                      ra_num,
                      ra_dt,
                      ra_valid_to,
                      spcl_hndl_Cd,
                      ixos_archive_id,
                      RA_UPDT_DT
                     )
              VALUES (
                      s_ra#ra_id.NEXTVAL,
                      v_cr_id,
                      rec_all_unprocess_ra_acks.cust_acct_id,
                      rec_all_unprocess_ra_acks.cust_cr_id,
                      rec_all_unprocess_ra_acks.cust_cr_uniq_id,
                      rec_all_unprocess_ra_acks.crte_dt,
                      rec_all_unprocess_ra_acks.rtn_po_num,
                      rec_all_unprocess_ra_acks.ra_num,
                      rec_all_unprocess_ra_acks.ra_dt,
                      nvl(rec_all_unprocess_ra_acks.ra_valid_to,rec_all_unprocess_ra_acks.ra_dt + 30),
                      rec_all_unprocess_ra_acks.spcl_hndl_cd,
                      rec_all_unprocess_ra_acks.ixos_archive_id,
                      sysdate
                     )
           RETURNING ra_id
                INTO v_ra_id;
         v_rows_processed := v_rows_processed + 1;
      END IF;
      DBMS_OUTPUT.put_line ('processs RA ' || v_ra_id);
   END process_ra_update;
-------------------------------------------------------------------------------------------------------------------
   PROCEDURE process_ra_item (
      ra_wrk_lin_tab   IN   ra_wrk_lin_type,
      v_ra_id          IN   s_ra.ra_id%TYPE,
      crte_dt          IN   s_ra_item.crte_dt%TYPE,
      updt_ra_table    IN   BOOLEAN
   )
   IS
      TYPE ra_id_type IS TABLE OF s_ra_item.ra_id%TYPE
         INDEX BY BINARY_INTEGER;
      ra_id_tab          ra_id_type;
      TYPE item_num_type IS TABLE OF s_ra_item.item_num%TYPE
         INDEX BY BINARY_INTEGER;
      item_num_tab       item_num_type;
      TYPE invc_num_type IS TABLE OF s_ra_item.invc_num%TYPE
         INDEX BY BINARY_INTEGER;
      invc_num_tab       invc_num_type;
      TYPE invc_dt_type IS TABLE OF s_ra_item.invc_dt%TYPE
         INDEX BY BINARY_INTEGER;
      invc_dt_tab        invc_dt_type;
      TYPE rtn_item_qty_type IS TABLE OF s_ra_item.rtn_item_qty%TYPE
         INDEX BY BINARY_INTEGER;
      rtn_item_qty_tab   rtn_item_qty_type;
      TYPE rsn_cd_type IS TABLE OF s_ra_item.rsn_cd%TYPE
         INDEX BY BINARY_INTEGER;
      rsn_cd_tab         rsn_cd_type;
      TYPE rtn_rjct_cd_type IS TABLE OF s_ra_item.rtn_rjct_cd%TYPE
         INDEX BY BINARY_INTEGER;
      rtn_rjct_cd_tab    rtn_rjct_cd_type;
   BEGIN
      -- If RA header exists then delete all the existing RA_ITEM data and insert again
      IF updt_ra_table = TRUE
      THEN
         DELETE FROM s_ra_item
               WHERE ra_id = v_ra_id;
      END IF;
      ra_id_tab.DELETE;
      item_num_tab.DELETE;
      invc_num_tab.DELETE;
      invc_dt_tab.DELETE;
      rtn_item_qty_tab.DELETE;
      rsn_cd_tab.DELETE;
      FOR i IN ra_wrk_lin_tab.FIRST .. ra_wrk_lin_tab.LAST
      LOOP
         ra_id_tab (i) := v_ra_id;
         item_num_tab (i) := ra_wrk_lin_tab (i).item_num;
         invc_num_tab (i) := ra_wrk_lin_tab (i).invc_num;
         invc_dt_tab (i) := ra_wrk_lin_tab (i).invc_dt;
         rtn_item_qty_tab (i) := ra_wrk_lin_tab (i).rtn_item_qty;
         rsn_cd_tab (i) := ra_wrk_lin_tab (i).rsn_cd;
         rtn_rjct_cd_tab (i) := ra_wrk_lin_tab (i).rtn_rjct_cd;
      END LOOP;
      FORALL i IN ra_id_tab.FIRST .. ra_id_tab.LAST
         INSERT INTO s_ra_item
                     (ra_item_id, ra_id,
                      item_num, invc_num, invc_dt,
                      rtn_item_qty, rsn_cd,
                      rtn_rjct_cd, crte_dt
                     )
              VALUES (s_ra_item#ra_item_id.NEXTVAL, ra_id_tab (i),
                      item_num_tab (i), invc_num_tab (i), invc_dt_tab (i),
                      rtn_item_qty_tab (i), rsn_cd_tab (i),
                      rtn_rjct_cd_tab (i), crte_dt
                     );
   END process_ra_item;
-------------------------------------------------------------------------------------------------------------------
   PROCEDURE update_ra_for_ixos
   IS
       v_cr_id      s_cr.cr_id%TYPE;
       v_ra_id      s_ra.ra_id%TYPE;
   BEGIN
      v_cr_id := 0;
      v_ra_id := 0;
      UPDATE s_ra ra
        SET ra.ixos_archive_id = rec_all_unprocess_ra_acks.ixos_archive_id,
            ra.ra_updt_dt = sysdate,
            ra.pnt_flg = rec_all_unprocess_ra_acks.pnt_flg
      WHERE ra.ra_num = rec_all_unprocess_ra_acks.ra_num
      RETURNING ra_id, cr_id
        into v_ra_id, v_cr_id;
      dbms_output.put_line('v_ra_id - ' || v_ra_id || ' ra_num - ' || rec_all_unprocess_ra_acks.ra_num);
      v_rows_processed := v_rows_processed + 1;
      IF (SQL%ROWCOUNT = 0)
      THEN
         -- Race condition - IXOS Ack arriving before RA Ack
         -- check if ack date is greater than 5 days, then mark ra_id as -2
         IF (trunc(rec_all_unprocess_ra_acks.crte_dt) < trunc(sysdate - 5))
         THEN
            UPDATE s_ra_wrk rw
                SET rw.ra_id = c_unprocesssed_ixos_ack,
                    rw.ra_updt_dts = sysdate
            WHERE ROWID = rec_all_unprocess_ra_acks.row_id;
            DBMS_OUTPUT.put_line ('Race Condition. IXOS ack arriving before RA ack.');
         END IF;
      ELSE -- IXOS Archive ID updated
          -- Update CR to Acknowledged status (R) only if CR is in Submitted status (S).
          -- If CR already in Finalized status (F), then don't update the CR status
          -- as credit memo might have arrived before RA.
          IF(v_cr_id IS NOT NULL OR v_cr_id <> 0) THEN
            UPDATE    s_cr cr
            SET cr.UPDT_DTS = sysdate,
                cr.cr_stat_cd =
                   CASE
                      WHEN cr_stat_cd = 'S'
                         THEN 'R'
                      ELSE cr_stat_cd
                   END
            WHERE cr_id = v_cr_id;
          ELSE
            UPDATE    s_cr cr
            SET cr.UPDT_DTS = sysdate,
                cr.cr_stat_cd =
                   CASE
                      WHEN cr_stat_cd = 'S'
                         THEN 'R'
                      ELSE cr_stat_cd
                   END
            WHERE cr.cust_acct_id = rec_all_unprocess_ra_acks.cust_acct_id
                AND cr.cust_cr_id = rec_all_unprocess_ra_acks.cust_cr_id
                AND cust_cr_uniq_id = rec_all_unprocess_ra_acks.cust_cr_uniq_id;
          END IF;
            dbms_output.put_line('Setting CR_ID to ACKED status ' || v_cr_id);
            -- Update RA Work table with RA_ID that was updated with IXOS Archive ID
          IF (v_ra_id = 0) THEN
            v_ra_id := c_process_error_ixos_ack;
          END IF;
            UPDATE s_ra_wrk rw
                SET rw.ra_id = v_ra_id,
                    rw.ra_updt_dts = sysdate
            WHERE ROWID = rec_all_unprocess_ra_acks.row_id;
      END IF;
   END update_ra_for_ixos;
---------------------------------------------------------------------------------------------------------------------
END pkg_ra_process_ELA;
/
