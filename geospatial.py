import os
import pandas as pd

class StatisticsLoader:
    def __init__(self, paths_by_year: dict[int, str], VARIABLE_DEFINITIONS: dict[str, str]
):

        self.paths_by_year = paths_by_year
        self.populations: dict[int, pd.DataFrame] = {}
        self.cleaned_populations: dict[int, pd.DataFrame] = {}
        self.activity: dict[int, pd.DataFrame] = {}
        self.cleaned_activity: dict[int, pd.DataFrame] = {}
        self.VARIABLE_DEFINITIONS = VARIABLE_DEFINITIONS
       

    def load_population_data(self, folder_path: str) -> pd.DataFrame:
        pop_file = None
        for f in os.listdir(folder_path):
            if "population" in f.lower():
                pop_file = os.path.join(folder_path, f)
                break

        if pop_file is None:
            raise FileNotFoundError(f"No Population file found in {folder_path}")

        ext = os.path.splitext(pop_file)[1].lower()

        if ext in [".xls", ".xlsx"]:
            for skip in range(10):
                df = pd.read_excel(pop_file, skiprows=skip)
                cols = map(str.lower, map(str, df.columns))
                if any(k in cols for k in ["iris", "reg", "dep", "p13_pop"]):
                    return df
            return pd.read_excel(pop_file)

        elif ext == ".csv":
            for enc in ["utf-8", "latin1", "cp1252"]:
                try:
                    with open(pop_file, encoding=enc) as f:
                        sample = f.read(2048)
                    sep = ";" if sample.count(";") > sample.count(",") else ","
                    df = pd.read_csv(pop_file, sep=sep, encoding=enc)
                    cols = map(str.lower, map(str, df.columns))
                    if any(k in cols for k in ["iris", "reg", "dep", "p17_pop"]):
                        return df
                except Exception:
                    continue
            raise ValueError(f"Could not read {pop_file}")

        else:
            raise ValueError(f"Unsupported file type: {ext}")

    def load_all_population(self):
        for year, path in self.paths_by_year.items():
            self.populations[year] = self.load_population_data(path)
 
    def cleanse_population(self):
        # --- First: clean 2017+ files (prefix-based) ---
        for year, df in self.populations.items():
            if year < 2017:
                continue

            prefix = f"P{str(year)[-2:]}_"
            cprefix = f"C{str(year)[-2:]}_"

            renamer = {}
            keep_cols = []

            for col in df.columns:
                if col == "IRIS":
                    keep_cols.append(col)
                elif col.startswith(prefix) or col.startswith(cprefix):
                    renamer[col] = col[4:]
                    keep_cols.append(col)

            cleaned_df = df[keep_cols].rename(columns=renamer)
            self.cleansed_populations[year] = cleaned_df

            print(f"✅ {year}: kept {len(cleaned_df.columns)} columns")

        # --- Use 2017 as the column reference ---
        reference_cols = list(self.cleansed_populations[2017].columns)

        # --- Clean 2013–2016 files ---
        for year, df in self.populations.items():
            if year >= 2017:
                continue

            filtered_df = df.loc[
                :, [c for c in df.columns if c == "IRIS" or "Pop" in c]
            ].copy()

            filtered_df.columns = reference_cols
            self.cleansed_populations[year] = filtered_df
 
    def load_activity_data(self, year: int, folder_path: str) -> pd.DataFrame:
        for ext in [".xlsx", ".xls", ".csv"]:
            file_path = os.path.join(folder_path, f"activity{ext}")
            if not os.path.exists(file_path):
                continue

            print(f"📂 Loading activity file for {year}: {file_path}")

            if ext == ".csv":
                return pd.read_csv(file_path, sep=";", low_memory=False)

            # Excel: detect header row
            for i in range(10):
                test_df = pd.read_excel(file_path, header=i, nrows=1)
                if "IRIS" in test_df.columns or "Code IRIS" in test_df.columns:
                    return pd.read_excel(file_path, header=i)

            # Fallback
            return pd.read_excel(file_path)

        raise FileNotFoundError(f"No activity file found for {year} in {folder_path}")
 
    def load_all_activity(self):
        for year, path in self.paths_by_year.items():
            try:
                self.activity[year] = self.load_activity_data(year, path)
                print(f"✅ Activity {year}: {len(self.activity[year])} rows")
            except FileNotFoundError as e:
                print(f"⚠️ {e}")
    
    def cleanse_activity(self, ACTIVITY_TARGET_COLS):
        for year, df in self.activity.items():

            # ---------------- Pre-2017 ---------------- #
            if year <= 2016:
                # Keep IRIS + first 24 activity columns
                cleaned_df = df.iloc[:, :len(ACTIVITY_TARGET_COLS)].copy()

            # ---------------- 2017+ ---------------- #
            else:
                prefix = f"P{str(year)[-2:]}_"

                cols_to_keep = ["IRIS"] + [
                    col for col in df.columns
                    if any(col == prefix + base for base in ACTIVITY_TARGET_COLS[1:])
                ]

                cleaned_df = df[cols_to_keep].copy()

                # Remove prefix
                cleaned_df.columns = [
                    col.replace(prefix, "") if col != "IRIS" else col
                    for col in cleaned_df.columns
                ]

            # ---------------- Standardize columns ---------------- #
            cleaned_df = cleaned_df.reindex(columns=ACTIVITY_TARGET_COLS)

            self.cleaned_activity[year] = cleaned_df

    def show_name_variables(self, contains: str | None = None):
        """
        Display variable codes with their full descriptive names.

        Parameters
        ----------
        contains : str, optional
            Filter variables containing this substring (e.g. 'POP', 'ACT', 'H', 'F')
        """

        variables = self.VARIABLE_DEFINITIONS

        if contains:
            contains = contains.upper()
            variables = {
                code: desc
                for code, desc in variables.items()
                if contains in code
            }

        if not variables:
            print("⚠️ No matching variables found.")
            return

        max_len = max(len(code) for code in variables)

        print("\n📘 Variable Definitions")
        print("─" * 70)
        for code, desc in sorted(variables.items()):
            print(f"{code:<{max_len}}  →  {desc}")
        print("─" * 70)
