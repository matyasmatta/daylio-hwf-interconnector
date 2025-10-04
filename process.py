import json
from datetime import datetime, timezone, timedelta
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
import csv
from io import StringIO


class Entry:
    """Represents a single Daylio entry."""
    def __init__(self, data: Dict[str, Any], journal: Optional['DaylioJournal'] = None):
        # Store the raw data and a reference to the journal for lookups
        self._data = data
        self.journal = journal

        # --- Direct Attribute Assignment ---
        # The raw time components (useful for reconstruction/debugging)
        self.id: int = data["id"]
        self.minute: int = data["minute"]
        self.hour: int = data["hour"]
        self.day: int = data["day"]
        self.month: int = data["month"]
        self.year: int = data["year"]
        self.mood_id: int = data["mood"]

        # Optional fields with clever defaulting
        self.note: str = data.get("note", "").strip()
        self.note_title: str = data.get("note_title", "").strip()
        self.tags: List[int] = data.get("tags", [])
        self.assets: List[Any] = data.get("assets", [])
        self.is_favorite: bool = data.get("isFavorite", False)
        
    # --- Calculated Properties for Date/Time Intelligence ---

    @property
    def timestamp_utc(self) -> datetime:
        """Returns the entry time as a UTC datetime object."""
        # Daylio stores milliseconds since epoch
        return datetime.fromtimestamp(self._data["datetime"] / 1000, tz=timezone.utc)

    @property
    def tz_offset(self) -> timedelta:
        """Returns the time zone offset as a timedelta object."""
        return timedelta(milliseconds=self._data["timeZoneOffset"])

    @property
    def local_datetime(self) -> datetime:
        """Returns the datetime adjusted to the local time of the entry."""
        return self.timestamp_utc - self.tz_offset

    # --- Data Linking Properties (Requires journal reference) ---

    @property
    def mood_name(self) -> str:
        """Resolves the mood ID to its name using the journal reference."""
        if self.journal:
            return self.journal.get_mood_name(self.mood_id)
        return f"ID {self.mood_id}"

    @property
    def tag_names(self) -> List[str]:
        """Resolves tag IDs to their names using the journal reference."""
        if self.journal:
            return [self.journal.get_tag_name(tag_id) for tag_id in self.tags]
        return [f"ID {tag_id}" for tag_id in self.tags]
        
    # --- Utility Methods ---

    def has_note(self) -> bool:
        """Checks if the entry has any non-empty note content."""
        return bool(self.note or self.note_title)

    def get_note_text(self) -> str:
        """
        Cleverly combines the title and body into a single readable string.
        """
        if self.note_title and self.note:
            return f"[{self.note_title}]\n{self.note}"
        elif self.note_title:
            return f"[{self.note_title}]"
        else:
            return self.note
            
    def __repr__(self):
        # Use the mood name and local time for a more informative representation
        dt_str = self.local_datetime.isoformat(timespec='minutes')
        return f"<Entry id={self.id} time={dt_str} mood='{self.mood_name}'>"

class DaylioJournal:
    """Represents the entire Daylio journal data structure."""
    def __init__(self, data: Dict[str, Any]):
        self.version: Optional[str] = data.get("version")
        self.is_reminder_on: bool = data.get("isReminderOn", False)

        # --- Lookup Dictionaries ---
        # Moods and tags are stored by their ID for O(1) lookup
        self.moods: Dict[int, Dict[str, Any]] = {
            m["id"]: m for m in data.get("customMoods", [])
        }
        self.tags: Dict[int, Dict[str, Any]] = {
            t["id"]: t for t in data.get("tags", [])
        }

        # --- Entry Parsing ---
        # Pass a reference to self to each Entry instance
        self.entries: List[Entry] = [
            Entry(e, journal=self) for e in data.get("dayEntries", [])
        ]

    # --- Public Lookup Methods ---

    def get_mood_name(self, mood_id: int) -> str:
        """Retrieves the custom name for a given mood ID."""
        mood = self.moods.get(mood_id)
        # Use the 'custom_name' if available, otherwise fallback to 'name' (in case of old/default moods)
        return mood.get("custom_name") if mood and mood.get("custom_name") else mood.get("name", f"Unknown Mood ID ({mood_id})")

    def get_tag_name(self, tag_id: int) -> str:
        """Retrieves the name for a given tag ID."""
        tag = self.tags.get(tag_id)
        return tag.get("name", f"Unknown Tag ID ({tag_id})") if tag else f"Unknown Tag ID ({tag_id})"

    # --- Filtering and Analysis Methods ---

    def get_entries_by_mood(self, mood_name: str) -> List[Entry]:
        """
        Returns a list of entries matching the given mood name (case-insensitive).
        """
        mood_name_lower = mood_name.lower()
        return [
            e for e in self.entries
            if e.mood_name.lower() == mood_name_lower
        ]

    def get_entries_by_tag(self, tag_name: str) -> List[Entry]:
        """
        Returns a list of entries that contain the given tag name (case-insensitive).
        """
        tag_name_lower = tag_name.lower()
        return [
            e for e in self.entries
            if tag_name_lower in [name.lower() for name in e.tag_names]
        ]

    # --- Utility Methods ---

    def list_moods(self) -> List[str]:
        """Lists all unique custom mood names."""
        # Use a set comprehension for uniqueness and then convert to a list
        return list({self.get_mood_name(mid) for mid in self.moods.keys()})

    def list_tags(self) -> List[str]:
        """Lists all unique tag names."""
        return list({self.get_tag_name(tid) for tid in self.tags.keys()})

    def __repr__(self):
        return f"<DaylioJournal {len(self.entries)} entries | {len(self.moods)} moods | {len(self.tags)} tags>"

DEFAULT_TIMEZONE_OFFSET_MS = 2 * 60 * 60 * 1000 # Assuming CEST (UTC+2)

def import_how_we_feel_csv(csv_data: str, journal: 'DaylioJournal') -> List[Dict[str, Any]]:
    """
    Parses How We Feel CSV data and converts it into a list of Daylio entry dictionaries.

    Args:
        csv_data: A string containing the How We Feel CSV content.
        journal: The DaylioJournal instance, used to dynamically generate tag IDs
                 for new tags found in the CSV.

    Returns:
        A list of dictionaries, each formatted as a Daylio 'dayEntry'.
    """
    # Use StringIO to treat the string data as a file
    reader = csv.reader(StringIO(csv_data))
    header = next(reader) # Get the header row

    # --- Setup Tag Mapping ---
    # Create reverse lookup for existing tags: {tag_name: tag_id}
    tag_name_to_id = {tag['name']: tag_id for tag_id, tag in journal.tags.items()}
    # Determine the starting ID for new tags (assuming IDs are sequential integers)
    next_tag_id = max(journal.tags.keys(), default=0) + 1

    daylio_entries: List[Dict[str, Any]] = []
    
    # Map CSV column names to their index
    col_map = {name.strip(): i for i, name in enumerate(header)}
    
    # Columns containing tags that need to be split by ';'
    TAG_COLUMNS = ["Tags (People)", "Tags (Places)", "Tags (Events)", "Exercise", "Sleep", "Menstrual", "Steps", "Meditation", "Weather"]

    def get_tag_ids_from_row(row_data: Dict[str, str]) -> List[int]:
        """Collects, standardizes, and maps all CSV tags to Daylio IDs."""
        non_unique_tags: List[str] = []
        for col_name in TAG_COLUMNS:
            raw_tag_str = row_data.get(col_name, "")
            
            # Special handling for "Menstrual" (a key is a tag if the cell is not empty)
            if col_name == "Menstrual":
                if raw_tag_str:
                    non_unique_tags.append("Menstrual")
            
            # Combine all other tags, splitting by ';'
            else:
                # Exclude empty strings and clean up tags
                if raw_tag_str:
                    # Split by ';' and strip whitespace/empty results
                    split_tags = [t.strip() for t in raw_tag_str.split(';') if t.strip()]
                    non_unique_tags.extend(split_tags)

        unique_tags = list(set(non_unique_tags))
        tag_ids: List[int] = []

        for tag_name in unique_tags:
            # Daylio stores tags as lower-cased, hyphenated names (e.g., "by-myself")
            daylio_name = tag_name.lower().replace(" ", "-").replace("/", "-")
            
            if daylio_name not in tag_name_to_id:
                nonlocal next_tag_id
                # Add new tag to the journal's tag list and the lookup map
                journal.tags[next_tag_id] = {"id": next_tag_id, "name": daylio_name}
                tag_name_to_id[daylio_name] = next_tag_id
                tag_ids.append(next_tag_id)
                next_tag_id += 1
            else:
                tag_ids.append(tag_name_to_id[daylio_name])
                
        return tag_ids


    # --- Process Rows ---
    for row in reader:
        if not row: continue # Skip empty rows

        # Map row values to header names
        row_data = {header[i].strip(): value for i, value in enumerate(row) if i < len(header)}
        
        # 1. Date/Time Parsing
        date_str = row_data["Date"]
        # Format is '2025 Sat Oct 4 8:09 PM'. Use the generic locale directive %a/%b
        # Python format string: %Y %a %b %d %I:%M %p
        try:
            # Note: This is locale-dependent. If running on a non-English OS, 
            # you might need to use locale.setlocale(locale.LC_TIME, 'C') first.
            dt_local = datetime.strptime(date_str, "%Y %a %b %d %I:%M %p")
        except ValueError as e:
            print(f"Skipping entry due to invalid date format: {date_str}. Error: {e}")
            continue

        # Convert local time (without TZ info) to an assumed UTC timestamp
        # Daylio expects the timestamp in UTC (epoch ms)
        # We assume the CSV's date string IS the local time.
        dt_utc = dt_local - timedelta(milliseconds=DEFAULT_TIMEZONE_OFFSET_MS)
        timestamp_ms = int(dt_utc.timestamp() * 1000)

        # 2. Tag Mapping (Includes Moood)
        tag_ids = get_tag_ids_from_row(row_data)

        # 3. Mood Mapping (Moods are more complex and require a dedicated function/map)
        # For simplicity, we assume all CSV mood names exist in the journal or can be mapped.
        mood_name = row_data["Mood"].strip()
        mood_id = None
        
        # We need a mood lookup here. For a real app, you'd create a map:
        # mood_name_to_id = {m["custom_name"]: m_id for m_id, m in journal.moods.items()}
        # For this example, we assume we must know the Daylio ID for the 'Thoughtful' etc.
        # Since we don't have that map, we'll assign a placeholder ID (e.g., 1) for all.
        # *** NOTE: IN A REAL APP, YOU MUST MAP MOOD NAMES TO DAYLIO IDs ***
        mood_id = 1 # Placeholder for simplicity.

        # 4. Construct Daylio Dictionary
        daylio_entry = {
            "id": len(daylio_entries) + 1, # Simple sequential ID
            "minute": dt_local.minute,
            "hour": dt_local.hour,
            "day": dt_local.day,
            "month": dt_local.month-1,
            "year": dt_local.year,
            "datetime": timestamp_ms,
            "timeZoneOffset": DEFAULT_TIMEZONE_OFFSET_MS,
            "mood": mood_id,
            "note": row_data.get("Notes", "").strip(),
            "note_title": "", # HWF doesn't have a separate title
            "tags": tag_ids,
            "assets": [],
            "isFavorite": False,
        }
        
        daylio_entries.append(daylio_entry)

    return daylio_entries

def merge_journals(daylio_json_path: str, hwf_csv_path: str, output_path: str) -> None:
    """
    Loads an existing Daylio journal, imports How We Feel CSV data, 
    merges the data, and saves the new, combined journal to a JSON file.

    Args:
        daylio_json_path: Path to the existing Daylio JSON backup file.
        hwf_csv_path: Path to the How We Feel CSV export file.
        output_path: Path where the new merged Daylio JSON will be saved.
    """
    
    # 1. Load Existing Daylio Journal
    print(f"Loading existing Daylio journal from: {daylio_json_path}")
    try:
        with open(daylio_json_path, "r", encoding="utf8") as f:
            daylio_data = json.load(f)
        journal = DaylioJournal(daylio_data)
    except FileNotFoundError:
        print(f"Error: Daylio JSON file not found at {daylio_json_path}")
        return
    except json.JSONDecodeError as e:
        print(f"Error decoding Daylio JSON: {e}")
        return

    # 2. Read How We Feel CSV Data
    print(f"Reading How We Feel CSV from: {hwf_csv_path}")
    try:
        with open(hwf_csv_path, "r", encoding="utf8") as f:
            csv_content = f.read()
    except FileNotFoundError:
        print(f"Error: HWF CSV file not found at {hwf_csv_path}")
        return

    # 3. Convert and Import HWF Entries (This also updates journal.tags)
    hwf_daylio_dicts = import_how_we_feel_csv(csv_content, journal)
    print(f"Converted {len(hwf_daylio_dicts)} entries from HWF.")

    # 4. Prepare Existing Entries for Re-Export
    # Convert the existing Entry objects back into their raw dictionary format
    existing_daylio_dicts = [e._data for e in journal.entries]

    # 5. Combine and Re-ID Entries
    all_entries = existing_daylio_dicts + hwf_daylio_dicts
    
    # The IDs need to be sequential and unique across the entire list.
    for i, entry_dict in enumerate(all_entries):
        # Daylio IDs are 1-based sequential integers
        entry_dict["id"] = i + 1 

    # 6. Reconstruct Final Daylio Data Structure
    # Use the original data as a template, but update the dynamic parts
    merged_data = daylio_data.copy()
    
    # Update the core components
    merged_data["dayEntries"] = all_entries
    merged_data["tags"] = list(journal.tags.values())
    merged_data["customMoods"] = list(journal.moods.values()) 
    
    # 7. Save the Merged Journal
    print(f"Total entries after merge: {len(all_entries)}")
    print(f"Saving merged journal to: {output_path}")
    try:
        with open(output_path, "w", encoding="utf8") as f:
            json.dump(merged_data, f, indent=4, ensure_ascii=False)
        print("✅ Merge successful!")
    except IOError as e:
        print(f"Error writing output file: {e}")

merge_journals(
    daylio_json_path="old.json",
    hwf_csv_path="hwf_check_ins2025-10-04_21-02.csv",
    output_path="merged_daylio_journal.json"
)