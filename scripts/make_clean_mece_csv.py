import csv
import os
import re
from typing import List, Dict

IN_PATH = os.path.join('data', 'raw_data', 'data_extraction_amr_ssi_ecsa.csv')
OUT_PATH = os.path.join('data', 'raw_data', 'data_extraction_amr_ssi_ecsa_cleaned_mece.csv')
OUT_ENRICHED = os.path.join('data', 'processed_data', 'data_extraction_amr_ssi_ecsa_enriched.csv')
OUT_REEXTRACT = os.path.join('data', 'processed_data', 'reextraction_targets.csv')


def tolower(x: str) -> str:
    return (x or '').lower()


def read_csv(path: str) -> List[Dict[str, str]]:
    # Use utf-8-sig to strip BOM if present
    with open(path, newline='', encoding='utf-8-sig') as f:
        rdr = csv.DictReader(f)
        if rdr.fieldnames is None:
            return []
        # Normalize headers: strip spaces and BOM remnants
        rdr.fieldnames = [fn.strip() if fn else '' for fn in rdr.fieldnames]
        rows = []
        for r in rdr:
            rows.append({(k.strip() if k else ''): v for k, v in r.items()})
        return rows


def write_csv(path: str, rows: List[Dict[str, str]], fieldnames: List[str]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, '') for k in fieldnames})


def normalize_design(val: str) -> str:
    x = tolower(val)
    if re.search(r"randomi[sz]ed|randomized controlled trial|randomised controlled trial|\brct\b", x):
        return 'RCT'
    if re.search(r"post-?hoc.*random|secondary analysis.*random|secondary.*rct", x):
        return 'Secondary analysis (RCT)'
    if re.search(r"before-?after|pre[- ]?post|pre.*post", x):
        return 'Before-after'
    if re.search(r"cohort|longitudinal|prospective observational cohort|prospective cohort", x):
        return 'Cohort'
    if re.search(r"cross[- ]?sectional|descriptive analysis|survey", x):
        return 'Cross-sectional'
    if re.search(r"surveillance", x):
        return 'Surveillance'
    if re.search(r"laboratory|laboratory-based", x):
        return 'Laboratory-based'
    if re.search(r"observational cohort", x):
        return 'Cohort'
    if re.search(r"multicentre|multicenter", x) and re.search(r"cross[- ]?sectional", x):
        return 'Cross-sectional'
    return 'Other'


def tag_by_patterns(text: str, patterns: Dict[str, str]) -> str:
    txt = tolower(text)
    tags: List[str] = []
    for name, pat in patterns.items():
        if re.search(pat, txt):
            tags.append(name)
    return '; '.join(tags) if tags else ''

# ---------- Derivable fields utilities ----------
MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6,
    'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12
}

def parse_years(period: str):
    s = tolower(period)
    years = re.findall(r"\b(?:19|20)\d{2}\b", s)
    ys = years[0] if years else ''
    ye = years[-1] if years else ''
    return ys, ye

def month_to_num(tok: str):
    return MONTHS.get(tolower(tok).strip(), None)

def parse_period_dates(period: str):
    s = tolower(period)
    m = re.findall(r"(\d{1,2})?\s*([a-z]+)\s*(\d{4})", s)
    if len(m) >= 1:
        d1 = m[0]
        day1 = d1[0] if d1[0] else '01'
        m1 = month_to_num(d1[1]) or 1
        y1 = d1[2]
        start = f"{y1}-{m1:02d}-{int(day1):02d}"
    else:
        ys, _ = parse_years(period)
        start = f"{ys}-01-01" if ys else ''
    if len(m) >= 2:
        d2 = m[-1]
        day2 = d2[0] if d2[0] else '28'
        m2 = month_to_num(d2[1]) or 12
        y2 = d2[2]
        end = f"{y2}-{m2:02d}-{int(day2):02d}"
    else:
        _, ye = parse_years(period)
        end = f"{ye}-12-31" if ye else ''
    return start, end

def parse_int_safe(x: str) -> str:
    if not x:
        return ''
    m = re.search(r"\d[\d,]*", x)
    if not m:
        return ''
    return str(int(m.group(0).replace(',', '')))

def parse_percent_safe(x: str) -> str:
    if not x:
        return ''
    m = re.search(r"\d+\.?\d*", x)
    return m.group(0) if m else ''

def yes_no_unclear(condition: bool, known: bool=True) -> str:
    if not known:
        return 'unclear'
    return 'yes' if condition else 'no'

def country_standardize(s: str) -> List[str]:
    if not s:
        return []
    parts = [p.strip() for p in re.split(r",|;|/|\band\b", s) if p.strip()]
    mapped = []
    for p in parts:
        if p.lower() in {"democratic republic of congo", "drc"}:
            mapped.append("Democratic Republic of the Congo")
        else:
            mapped.append(p)
    return mapped

def facility_level_guess(setting: str) -> str:
    s = tolower(setting)
    if 'tertiary' in s or 'referral' in s or 'teaching' in s:
        return 'tertiary/referral/teaching'
    if 'district' in s:
        return 'district'
    if 'private' in s:
        return 'private'
    if 'regional' in s:
        return 'regional'
    return 'unspecified'

def map_specialities(s: str) -> str:
    s0 = tolower(s)
    cats = []
    if 'obst' in s0 or 'gyn' in s0:
        cats.append('OBGYN')
    if 'general' in s0:
        cats.append('General Surgery')
    if 'orthop' in s0 or 'trauma' in s0:
        cats.append('Orthopedics/Trauma')
    if 'pediatr' in s0 or 'paediatr' in s0:
        cats.append('Pediatrics')
    if 'urolog' in s0:
        cats.append('Urology')
    if 'head and neck' in s0 or 'head & neck' in s0:
        cats.append('Head & Neck')
    if not cats and s.strip():
        cats.append('Other')
    return '; '.join(sorted(set(cats))) if cats else ''

def map_procedures(s: str) -> str:
    s0 = tolower(s)
    tags = []
    if 'cesarean' in s0 or 'caesarean' in s0 or 'c-section' in s0:
        tags.append('Cesarean section')
    if 'laparotomy' in s0:
        tags.append('Laparotomy')
    if 'debridement' in s0:
        tags.append('Debridement')
    if 'orif' in s0 or 'open reduction' in s0:
        tags.append('ORIF')
    if 'amputation' in s0:
        tags.append('Amputation')
    if 'hernia' in s0:
        tags.append('Hernia repair')
    if 'appendect' in s0:
        tags.append('Appendectomy')
    return '; '.join(sorted(set(tags))) if tags else ''

def pathogen_std(name: str) -> str:
    s = tolower(name)
    if not s:
        return ''
    s = s.replace('coagulase negative staphylococci', 'cons').replace('coagulase-negative staphylococci', 'cons')
    if 'sciuri' in s:
        return 'Mammaliicoccus sciuri'
    if 'cons' in s or 'coagulase' in s:
        return 'CoNS'
    if 's. aureus' in s or 'staphylococcus aureus' in s:
        return 'Staphylococcus aureus'
    if 'klebsiella' in s:
        return 'Klebsiella pneumoniae' if 'pneumon' in s else 'Klebsiella spp.'
    if 'e. coli' in s or 'escherichia coli' in s:
        return 'Escherichia coli'
    if 'pseudomonas' in s:
        return 'Pseudomonas aeruginosa'
    if 'acinetobacter' in s:
        return 'Acinetobacter baumannii' if 'baumannii' in s else 'Acinetobacter spp.'
    if 'enterococcus' in s:
        return 'Enterococcus spp.'
    if 'proteus' in s:
        return 'Proteus spp.'
    if 'citrobacter' in s:
        return 'Citrobacter spp.'
    if 'enterobacter' in s:
        return 'Enterobacter spp.'
    return name.strip()


def main():
    rows = read_csv(IN_PATH)
    if not rows:
        raise SystemExit('Input CSV appears empty')

    # Deduplicate by Author + Year + Title
    key_cols = ['Author', 'Year of publication', 'Title of paper']
    for c in key_cols:
        if c not in rows[0]:
            raise SystemExit(f'Missing expected column: {c}')

    seen = set()
    dedup: List[Dict[str, str]] = []
    for r in rows:
        k = f"{r.get(key_cols[0], '')}||{r.get(key_cols[1], '')}||{r.get(key_cols[2], '')}"
        if k not in seen:
            seen.add(k)
            dedup.append(r)

    # Add Study Design (MECE)
    for r in dedup:
        r['Study Design (MECE)'] = normalize_design(r.get('Study Design', ''))

    # Define thematic patterns
    drivers_patterns = {
        'Antibiotic misuse/overuse': r"misuse|overuse|irrational|injudicious|inappropriate|empiric|empirical|broad-?spectrum",
        'OTC/no-prescription access': r"over[- ]?the[- ]?counter|otc|without (a )?prescription|sold without prescription|self-?medication|self[- ]?treatment",
        'Poor IPC/asepsis': r"poor (infection|aseptic)|lack of aseptic|sterilization|hygiene|hand|contamination|asepsis|ipc|infection control",
        'Prolonged prophylaxis': r"prolong(ed|ed) prophylaxis|long duration prophylaxis|post[- ]?operative antibiotics",
        'Lack of stewardship': r"lack of (antimicrobial )?stewardship|no stewardship|absence of stewardship",
        'Limited diagnostics/lab': r"lack of (routine )?culture|no microbiolog|limited diagnostic|lack of (laborator|lab) capacity",
        'Long stay/overcrowding': r"prolonged hospital stay|overcrowd|long(er)? hospital stay",
        'Guideline non-adherence': r"lack of adherence to (guidelines|protocol)|non[- ]adherence",
        'Supply/drug quality': r"poor quality drug|dumping|supply chain",
        'Community factors': r"community|drug pressure",
    }

    interv_patterns = {
        'IPC measures': r"infection prevention|ipc|operating theatre discipline|skin disinfection|steriliz",
        'AMS program': r"antimicrobial stewardship|ams",
        'Surveillance (SSI/AMR)': r"surveillance|monitoring|periodic profiling",
        'Guidelines/protocols': r"guideline|protocol|policy|standard operating",
        'Pre-incision prophylaxis': r"pre[- ]?incision|prophylaxis.*30[- ]?60",
        'Education/training': r"training|education|seminar|on-?job",
        'Diagnostics/AST expansion': r"culture|susceptibility testing|ast|diagnostic",
        'Surgical practice change': r"tricosan|triclosan|skin preparation|glove|instrument change|suture",
        'Capacity building/lab': r"laboratory capability|accreditation|slipta|capacity",
    }

    gaps_patterns = {
        'Anaerobes/fungi not assessed': r"anaerob|fung",
        'Limited surveillance/data': r"limited (surveillance|data)|lack of (surveillance|data)|paucity",
        'No molecular testing': r"lack of molecular|genomic|molecular epidemiolog",
        'Small/limited generalizability': r"small sample|single (center|centre)|limited generalizability|short study",
        'Lost to follow-up': r"loss to follow|lost to follow",
        'No standardized diagnosis': r"lack of standardized diagnosis|diagnostic criteria",
        'Missing outcomes/economic': r"no data on (mortality|economic|cost|length of stay|re-?operation|readmission)",
        'Resource constraints': r"resource constraint|financial",
        'Policy/guideline gaps': r"lack of (guideline|policy)",
    }

    policy_patterns = {
        'National surveillance absent': r"no (national )?surveillance|lack of surveillance system",
        'AMS absent/needed': r"stewardship (needed|lacking|absent)|need for ams",
        'Lab capacity limited': r"limited laborator|lack of laborator|no routine (ast|culture)",
        'Accreditation/improving': r"slipta|accredit|quality control",
        'Guidelines present/absent': r"guideline|protocol",
        'Regulatory weak': r"regulator(y)? (weak|absence)|over the counter|without prescription",
        'Coordination needed': r"central(ized)? body|coordinating body",
    }

    econ_dir_col = 'Economic - direct costs'
    econ_ind_col = 'Economic - indirect costs'

    for r in dedup:
        r['Drivers_AMR_Themes'] = tag_by_patterns(r.get('Reported Drivers of AMR', ''), drivers_patterns)
        r['Interventions_Themes'] = tag_by_patterns(r.get('Interventions/Innovations Described', ''), interv_patterns)
        r['Gaps_Themes'] = tag_by_patterns(r.get('Gaps Identified by Authors', ''), gaps_patterns)
        r['Policy_Capacity_Themes'] = tag_by_patterns(r.get('Policy Response/Capacity', ''), policy_patterns)
        econ_combined = f"{tolower(r.get(econ_dir_col, ''))} {tolower(r.get(econ_ind_col, ''))}"
        econ_patterns = {
            'Hospital cost savings': r"cost saving|savings",
            'Patient out-of-pocket': r"out[- ]?of[- ]?pocket|personal saving|family|caregiver|income",
            'Not reported': r"not applicable|not reported|no data|not available",
            'Catastrophic expenditures': r"catastrophic",
            'LOS/bed-day burden': r"length of stay|bed[- ]?day",
        }
        r['Economic_Costs_Themes'] = tag_by_patterns(econ_combined, econ_patterns)

    # Derivable fields
    for r in dedup:
        # Study period
        ys, ye = parse_years(r.get('Study Period', ''))
        r['year_start'] = ys
        r['year_end'] = ye
        ps, pe = parse_period_dates(r.get('Study Period', ''))
        r['period_start_date'] = ps
        r['period_end_date'] = pe

        # Numeric sanitation
        r['total_sample_n_parsed'] = parse_int_safe(r.get('Total Sample Size (N)', ''))
        r['total_procedures_n_parsed'] = parse_int_safe(r.get('Total Procedures', ''))
        r['total_ssis_n_parsed'] = parse_int_safe(r.get('Total SSIs', ''))
        r['total_ssi_isolates_n_parsed'] = parse_int_safe(r.get('Total SSI Isolates', ''))
        r['sex_female_pct_parsed'] = parse_percent_safe(r.get('Sex (%Female)', ''))
        r['adherence_guidelines_pct_parsed'] = parse_percent_safe(r.get('Adherence to Guidelines (%)', ''))
        r['ssi_incidence_pct_parsed'] = parse_percent_safe(r.get('SSI Incidence Rate', ''))

        # Diagnosis flags
        method = tolower(r.get('Method of SSI Diagnosis', ''))
        r['diagnosis_cdc_guidelines'] = yes_no_unclear('cdc' in method, known=bool(method))
        r['lab_culture_confirmed'] = yes_no_unclear('culture' in method, known=bool(method))
        r['followup_30d'] = 'yes' if re.search(r"30[- ]?day|30\s*days|day 30", method) else ('no' if method else 'unclear')

        # Denominator type
        r['ssi_denominator_type'] = (
            'procedures' if r['total_procedures_n_parsed'] else (
                'SSI cohort only' if tolower(r.get('Population Description','')).find('ssi') != -1 else 'unclear')
        )

        # Countries and setting
        r['country_list'] = '; '.join(country_standardize(r.get('Country/Countries','')))
        r['facility_level_guess'] = facility_level_guess(r.get('Setting',''))

        # Specialty and procedures
        r['speciality_set'] = map_specialities(r.get('Surgical Speciality',''))
        r['procedure_group'] = map_procedures(r.get('Specific Procedures',''))

        # Pathogen std
        r['pathogen1_std'] = pathogen_std(r.get('Pathogen 1 Name (Name of the most common isolated pathogen)',''))
        r['pathogen2_std'] = pathogen_std(r.get('Pathogen 2 Name (Name of the 2nd most common pathogen)',''))
        r['pathogen3_std'] = pathogen_std(r.get('Pathogen 3 Name (Name of the 3rd most common isolated pathogen)',''))

        # Outcome presence flags (for re-extraction targeting)
        def has_num(col):
            return bool(parse_percent_safe(r.get(col,'') or parse_int_safe(r.get(col,''))))
        r['has_mortality_data'] = 'yes' if any(has_num(c) for c in ['Mortality - SSI attributable rate (%)','Mortality - 30-day post-op','Mortality - 90-day post-op (%)']) else 'no'
        r['has_readmission_data'] = 'yes' if has_num('Morbidity - Readmission rate (%)') else 'no'
        r['has_reoperation_data'] = 'yes' if has_num('Morbidity - Re-opertation rate (%)') else 'no'
        r['has_los_data'] = 'yes' if (r.get('Hospital burden - Total length of stay (days)','') or r.get('Morbidity - Additional Hospital Stay (days)','')) else 'no'
        r['has_economic_costs'] = 'yes' if (r.get('Economic - direct costs','') or r.get('Economic - indirect costs','')) else 'no'

    # Ensure stable column order: original + new columns at the end
    original_fields = list(rows[0].keys())
    new_fields = [
        'Study Design (MECE)', 'Drivers_AMR_Themes', 'Interventions_Themes',
        'Gaps_Themes', 'Policy_Capacity_Themes', 'Economic_Costs_Themes',
        'year_start','year_end','period_start_date','period_end_date',
        'total_sample_n_parsed','total_procedures_n_parsed','total_ssis_n_parsed','total_ssi_isolates_n_parsed',
        'sex_female_pct_parsed','adherence_guidelines_pct_parsed','ssi_incidence_pct_parsed',
        'diagnosis_cdc_guidelines','lab_culture_confirmed','followup_30d','ssi_denominator_type',
        'country_list','facility_level_guess','speciality_set','procedure_group',
        'pathogen1_std','pathogen2_std','pathogen3_std',
        'has_mortality_data','has_readmission_data','has_reoperation_data','has_los_data','has_economic_costs'
    ]
    fieldnames = original_fields + [c for c in new_fields if c not in original_fields]

    write_csv(OUT_PATH, dedup, fieldnames)
    print(f"Wrote cleaned CSV with MECE tags to: {OUT_PATH}")

    # Also write enriched CSV to processed_data
    write_csv(OUT_ENRICHED, dedup, fieldnames)
    print(f"Wrote enriched CSV with derivable fields to: {OUT_ENRICHED}")

    # Build re-extraction shortlist
    shortlist_cols = [
        'Author','Year of publication','Title of paper','Country/Countries','Study Design','Study Design (MECE)',
        'total_procedures_n_parsed','total_ssis_n_parsed','ssi_incidence_pct_parsed',
        'has_mortality_data','has_readmission_data','has_reoperation_data','has_los_data','has_economic_costs',
        'Method of SSI Diagnosis','Economic - direct costs','Economic - indirect costs'
    ]
    re_rows = []
    for r in dedup:
        if any(r.get(k,'no') == 'no' for k in ['has_mortality_data','has_readmission_data','has_reoperation_data','has_los_data','has_economic_costs']):
            re_rows.append({k: r.get(k,'') for k in shortlist_cols})
    if re_rows:
        write_csv(OUT_REEXTRACT, re_rows, shortlist_cols)
        print(f"Wrote re-extraction shortlist to: {OUT_REEXTRACT}")


if __name__ == '__main__':
    main()
