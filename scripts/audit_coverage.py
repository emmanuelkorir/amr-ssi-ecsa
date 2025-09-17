import csv
import os
import re
from collections import Counter, defaultdict

IN_PATH = os.path.join('data','raw_data','data_extraction_amr_ssi_ecsa_cleaned_mece.csv')

# Fields that can be derived from existing columns (best-effort)
DERIVABLE = {
  'year_start': ['Study Period'],
  'year_end': ['Study Period'],
  'period_start_date': ['Study Period'],
  'period_end_date': ['Study Period'],
  'total_sample_n_parsed': ['Total Sample Size (N)'],
  'total_procedures_n_parsed': ['Total Procedures'],
  'total_ssis_n_parsed': ['Total SSIs'],
  'total_ssi_isolates_n_parsed': ['Total SSI Isolates'],
  'sex_female_pct_parsed': ['Sex (%Female)'],
  'adherence_guidelines_pct_parsed': ['Adherence to Guidelines (%)'],
  'ssi_incidence_pct_parsed': ['SSI Incidence Rate'],
  'diagnosis_cdc_guidelines': ['Method of SSI Diagnosis'],
  'lab_culture_confirmed': ['Method of SSI Diagnosis'],
  'followup_30d': ['Method of SSI Diagnosis'],
  'country_list': ['Country/Countries'],
  'facility_level_guess': ['Setting'],
  'speciality_set': ['Surgical Speciality'],
  'procedure_group': ['Specific Procedures'],
  'pathogen1_std': ['Pathogen 1 Name (Name of the most common isolated pathogen)'],
  'pathogen2_std': ['Pathogen 2 Name (Name of the 2nd most common pathogen)'],
  'pathogen3_std': ['Pathogen 3 Name (Name of the 3rd most common isolated pathogen)'],
}

# Fields likely NOT derivable without going back to sources
NOT_DERIVABLE = [
  'econ_direct_numeric', 'econ_indirect_numeric', 'mortality_attr_ssi_pct', 'mortality_30d_pct',
  'mortality_90d_pct', 'readmission_rate_pct', 'reoperation_rate_pct', 'additional_los_days',
]


def has_value(v: str) -> bool:
  return bool(v and str(v).strip() and str(v).strip().lower() not in {'not applicable','na','n/a','not available','not reported'})


def main():
  if not os.path.exists(IN_PATH):
    raise SystemExit(f'File not found: {IN_PATH}')
  with open(IN_PATH, newline='', encoding='utf-8-sig') as f:
    rdr = csv.DictReader(f)
    rows = list(rdr)
    fieldnames = list(rdr.fieldnames or [])

  # Coverage for derivable: whether source fields exist and have some value
  derivable_cov = {}
  for out_field, src_fields in DERIVABLE.items():
    present = all(sf in fieldnames for sf in src_fields)
    if not present:
      derivable_cov[out_field] = {'status':'missing_source_fields','coverage':0}
      continue
    count = sum(1 for r in rows if any(has_value(r.get(sf,'')) for sf in src_fields))
    derivable_cov[out_field] = {'status':'derivable', 'coverage': count/len(rows) if rows else 0.0}

  # Non-derivable: check if any existing columns could proxy
  # Simple heuristic: if any column already looks like it, we mark partial
  nd_cov = {}
  for fld in NOT_DERIVABLE:
    # attempt to see if a near match exists in existing fields
    pattern = re.compile(r'(mortality|readmission|re-?operation|length of stay|economic|cost|los|day)', re.I)
    possible = [c for c in fieldnames if pattern.search(c)]
    nd_cov[fld] = {'status':'needs_re-extraction','possible_existing_cols': possible}

  print('=== Derivable fields (best-effort) coverage ===')
  for k,v in sorted(derivable_cov.items()):
    print(f"{k}: {v['status']} | coverage={v['coverage']:.0%}")

  print('\n=== Likely not-derivable without source re-extraction ===')
  for k,v in sorted(nd_cov.items()):
    poss = ', '.join(v['possible_existing_cols'][:5])
    print(f"{k}: {v['status']} | possible_existing_cols: {poss}")


if __name__ == '__main__':
  main()
