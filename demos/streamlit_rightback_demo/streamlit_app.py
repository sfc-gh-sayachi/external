import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import re


st.set_page_config(page_title="Rightback Demo - Employees", layout="wide")
st.title("Employees Write-back Demo")


@st.cache_data(show_spinner=False)
def load_employees() -> pd.DataFrame:
    session = get_active_session()
    df = session.sql(
        """
        SELECT EMPLOYEE_ID,
               FIRST_NAME,
               LAST_NAME,
               EMAIL,
               DEPARTMENT,
               FUNCTION,
               TITLE,
               LOCATION,
               MANAGER_EMAIL,
               HIRE_DATE,
               ACTIVE,
               EMPLOYEE_UID,
               VERSION_NUMBER,
               IS_CURRENT
        FROM HRDEMO.EMPLOYEES
        WHERE IS_CURRENT = TRUE
        ORDER BY LAST_NAME, FIRST_NAME
        """
    ).to_pandas()
    # Normalize dtypes for editing
    text_cols = [
        "EMPLOYEE_ID",
        "FIRST_NAME",
        "LAST_NAME",
        "EMAIL",
        "DEPARTMENT",
        "FUNCTION",
        "TITLE",
        "LOCATION",
        "MANAGER_EMAIL",
        ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("")
    if "ACTIVE" in df.columns:
        df["ACTIVE"] = df["ACTIVE"].astype("boolean").fillna(True)
    if "HIRE_DATE" in df.columns:
        # Ensure pandas datetime type (date only is fine; Streamlit DateColumn will handle)
        df["HIRE_DATE"] = pd.to_datetime(df["HIRE_DATE"], errors="coerce")
    # Versioning meta
    for meta in ["EMPLOYEE_UID", "VERSION_NUMBER", "IS_CURRENT"]:
        if meta in df.columns:
            # Keep types consistent for editor but hide later
            if meta == "VERSION_NUMBER":
                df[meta] = pd.to_numeric(df[meta], errors="coerce").fillna(1).astype(int)
            elif meta == "IS_CURRENT":
                df[meta] = df[meta].astype("boolean").fillna(True)
            else:
                df[meta] = df[meta].astype("string").fillna("")
    return df


def refresh_data():
    load_employees.clear()


def validate_rows(df: pd.DataFrame) -> list[str]:
    errors: list[str] = []
    email_set: set[str] = set()
    email_regex = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    for idx, row in df.iterrows():
        first = str(row.get("FIRST_NAME", "")).strip()
        last = str(row.get("LAST_NAME", "")).strip()
        email = str(row.get("EMAIL", "")).strip().lower()
        if not first:
            errors.append(f"Row {idx + 1}: FIRST_NAME is required")
        if not last:
            errors.append(f"Row {idx + 1}: LAST_NAME is required")
        if not email:
            errors.append(f"Row {idx + 1}: EMAIL is required")
        elif not email_regex.match(email):
            errors.append(f"Row {idx + 1}: EMAIL '{email}' is invalid")
        elif email in email_set:
            errors.append(f"Row {idx + 1}: duplicate EMAIL '{email}' in editor")
        else:
            email_set.add(email)
    return errors


def perform_writeback(original_df: pd.DataFrame, edited_df: pd.DataFrame) -> tuple[int, int, int]:
    session = get_active_session()

    # Normalize
    def norm(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        text_cols = [
            "EMPLOYEE_ID",
            "FIRST_NAME",
            "LAST_NAME",
            "EMAIL",
            "DEPARTMENT",
            "FUNCTION",
            "TITLE",
            "LOCATION",
            "MANAGER_EMAIL",
        ]
        for c in text_cols:
            if c in out.columns:
                out[c] = out[c].astype("string").fillna("")
        if "ACTIVE" in out.columns:
            out["ACTIVE"] = out["ACTIVE"].astype("boolean").fillna(True)
        if "HIRE_DATE" in out.columns:
            out["HIRE_DATE"] = pd.to_datetime(out["HIRE_DATE"], errors="coerce")
        return out

    original_df = norm(original_df)
    edited_df = norm(edited_df)

    orig_by_id = {r["EMPLOYEE_ID"]: r for r in original_df.to_dict("records")}
    edited_records = edited_df.to_dict("records")

    edited_ids = {r["EMPLOYEE_ID"] for r in edited_records if r.get("EMPLOYEE_ID")}
    delete_ids = [rid for rid in orig_by_id.keys() if rid and rid not in edited_ids]

    updates: list[dict] = []
    inserts: list[dict] = []

    def normalize_email(e: str) -> str:
        return (e or "").strip().lower()

    for rec in edited_records:
        eid = rec.get("EMPLOYEE_ID", "")
        if eid and eid in orig_by_id:
            before = orig_by_id[eid]
            changed = False
            fields = [
                "FIRST_NAME",
                "LAST_NAME",
                "EMAIL",
                "DEPARTMENT",
                "FUNCTION",
                "TITLE",
                "LOCATION",
                "MANAGER_EMAIL",
            ]
            for f in fields:
                if str(rec.get(f, "")).strip() != str(before.get(f, "")).strip():
                    changed = True
                    break
            if not changed:
                # Compare date and boolean
                if pd.to_datetime(rec.get("HIRE_DATE")) != pd.to_datetime(before.get("HIRE_DATE")):
                    changed = True
                if bool(rec.get("ACTIVE")) != bool(before.get("ACTIVE")):
                    changed = True
            if changed:
                updates.append(rec)
        else:
            # New row when no EMPLOYEE_ID
            if rec.get("FIRST_NAME") or rec.get("LAST_NAME") or rec.get("EMAIL"):
                inserts.append(rec)

    deleted = 0
    updated = 0
    inserted = 0

    session.sql("BEGIN").collect()
    try:
        # Soft-delete (archive) by marking previous current row as not current
        for rid in delete_ids:
            session.sql(
                f"UPDATE HRDEMO.EMPLOYEES SET IS_CURRENT = FALSE, UPDATE_USER = CURRENT_USER(), UPDATE_DATE_TIME = CURRENT_TIMESTAMP() WHERE EMPLOYEE_ID = '{rid}'"
            ).collect()
            deleted += 1

        for rec in updates:
            # Create a new version row; mark old as not current
            eid = str(rec.get("EMPLOYEE_ID", "")).replace("'", "''")
            def q(x: str) -> str:
                return (str(x or "").strip()).replace("'", "''")
            first = q(rec.get("FIRST_NAME"))
            last = q(rec.get("LAST_NAME"))
            email = q(normalize_email(rec.get("EMAIL")))
            dept = q(rec.get("DEPARTMENT"))
            func = q(rec.get("FUNCTION"))
            title = q(rec.get("TITLE"))
            loc = q(rec.get("LOCATION"))
            mgr = q(normalize_email(rec.get("MANAGER_EMAIL")))
            hire = rec.get("HIRE_DATE")
            hire_str = pd.to_datetime(hire).strftime("%Y-%m-%d") if pd.notna(hire) else None
            active_val = "TRUE" if bool(rec.get("ACTIVE")) else "FALSE"
            # Determine versioning metadata from original row
            before = orig_by_id.get(rec.get("EMPLOYEE_ID"))
            uid = q(before.get("EMPLOYEE_UID")) if before and before.get("EMPLOYEE_UID") else None
            current_version = int(before.get("VERSION_NUMBER")) if before and before.get("VERSION_NUMBER") is not None else 1
            next_version = current_version + 1

            # Mark previous row as not current
            session.sql(
                f"UPDATE HRDEMO.EMPLOYEES SET IS_CURRENT = FALSE, UPDATE_USER = CURRENT_USER(), UPDATE_DATE_TIME = CURRENT_TIMESTAMP() WHERE EMPLOYEE_ID = '{eid}'"
            ).collect()

            # Insert new version row
            if uid:
                session.sql(
                    f"""
                    INSERT INTO HRDEMO.EMPLOYEES
                        (EMPLOYEE_UID, VERSION_NUMBER, IS_CURRENT,
                         FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, FUNCTION, TITLE, LOCATION, MANAGER_EMAIL, HIRE_DATE, ACTIVE)
                    VALUES
                        ('{uid}', {next_version}, TRUE,
                         '{first}', '{last}', '{email}', '{dept}', '{func}', '{title}', '{loc}', '{mgr}', {f"TO_DATE('{hire_str}')" if hire_str else 'NULL'}, {active_val})
                    """
                ).collect()
            else:
                # Fallback: let EMPLOYEE_UID default if not present
                session.sql(
                    f"""
                    INSERT INTO HRDEMO.EMPLOYEES
                        (VERSION_NUMBER, IS_CURRENT,
                         FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, FUNCTION, TITLE, LOCATION, MANAGER_EMAIL, HIRE_DATE, ACTIVE)
                    VALUES
                        ({next_version}, TRUE,
                         '{first}', '{last}', '{email}', '{dept}', '{func}', '{title}', '{loc}', '{mgr}', {f"TO_DATE('{hire_str}')" if hire_str else 'NULL'}, {active_val})
                    """
                ).collect()
            updated += 1

        for rec in inserts:
            def q(x: str) -> str:
                return (str(x or "").strip()).replace("'", "''")
            first = q(rec.get("FIRST_NAME"))
            last = q(rec.get("LAST_NAME"))
            email = q(normalize_email(rec.get("EMAIL")))
            dept = q(rec.get("DEPARTMENT"))
            func = q(rec.get("FUNCTION"))
            title = q(rec.get("TITLE"))
            loc = q(rec.get("LOCATION"))
            mgr = q(normalize_email(rec.get("MANAGER_EMAIL")))
            hire = rec.get("HIRE_DATE")
            hire_str = pd.to_datetime(hire).strftime("%Y-%m-%d") if pd.notna(hire) else None
            active_val = "TRUE" if bool(rec.get("ACTIVE", True)) else "FALSE"
            session.sql(
                f"""
                INSERT INTO HRDEMO.EMPLOYEES
                    (VERSION_NUMBER, IS_CURRENT,
                     FIRST_NAME, LAST_NAME, EMAIL, DEPARTMENT, FUNCTION, TITLE, LOCATION, MANAGER_EMAIL, HIRE_DATE, ACTIVE)
                VALUES
                    (1, TRUE,
                     '{first}', '{last}', '{email}', '{dept}', '{func}', '{title}', '{loc}', '{mgr}', {f"TO_DATE('{hire_str}')" if hire_str else 'NULL'}, {active_val})
                """
            ).collect()
            inserted += 1

        session.sql("COMMIT").collect()
    except Exception:
        session.sql("ROLLBACK").collect()
        raise

    return deleted, updated, inserted


with st.container(border=True):
    st.subheader("Employees")
    src_df = load_employees()

    st.caption("Add, edit, or delete employees. EMAIL must be unique and valid.")

    edited_df = st.data_editor(
        src_df,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "EMPLOYEE_ID": st.column_config.Column("EMPLOYEE_ID", disabled=True),
            "FIRST_NAME": st.column_config.Column("FIRST_NAME"),
            "LAST_NAME": st.column_config.Column("LAST_NAME"),
            "EMAIL": st.column_config.Column("EMAIL"),
            "DEPARTMENT": st.column_config.Column("DEPARTMENT"),
            "FUNCTION": st.column_config.Column("FUNCTION"),
            "TITLE": st.column_config.Column("TITLE"),
            "LOCATION": st.column_config.Column("LOCATION"),
            "MANAGER_EMAIL": st.column_config.Column("MANAGER_EMAIL"),
            "HIRE_DATE": st.column_config.DateColumn("HIRE_DATE", format="YYYY-MM-DD"),
            "ACTIVE": st.column_config.CheckboxColumn("ACTIVE", default=True),
        },
        key="employees_editor",
    )

    col_a, col_b = st.columns([1, 3])
    with col_a:
        if st.button("Save changes", type="primary"):
            errors = validate_rows(edited_df)
            if errors:
                st.error("\n".join(errors))
            else:
                with st.spinner("Writing changes..."):
                    try:
                        deleted, updated, inserted = perform_writeback(src_df, edited_df)
                        refresh_data()
                        st.success(
                            f"Done. Deleted: {deleted} • Updated: {updated} • Inserted: {inserted}"
                        )
                        st.rerun()
                    except Exception as ex:
                        st.error(f"Failed to save changes: {ex}")

    with col_b:
        if st.button("Refresh"):
            refresh_data()
            st.rerun()


