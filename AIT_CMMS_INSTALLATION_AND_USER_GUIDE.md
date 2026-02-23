# AIT CMMS — Installation & User Guide

**AIT Complete CMMS — Computerized Maintenance Management System**

---

## TABLE OF CONTENTS

1. [What You Will Need](#1-what-you-will-need)
2. [Step 1 — Install Python](#2-step-1--install-python)
3. [Step 2 — Install Required Libraries](#3-step-2--install-required-libraries)
4. [Step 3 — Set Up the Program Files](#4-step-3--set-up-the-program-files)
5. [Step 4 — Run the Program](#5-step-4--run-the-program)
6. [Logging In](#6-logging-in)
7. [Navigating the System](#7-navigating-the-system)
8. [Common Tasks](#8-common-tasks)
9. [Running Reports](#9-running-reports)
10. [Backing Up Your Data](#10-backing-up-your-data)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. What You Will Need

- A Windows PC (Windows 10 or 11 recommended)
- Internet access (one time only, for installation)
- The `SQLite_CMMS-main` folder provided to you (received via email or download link)

No prior technical knowledge is required. Follow each step in order.

---

## 2. Step 1 — Install Python

Python is the language this program is written in. You only need to install it once.

1. Open your web browser and go to:
   **https://www.python.org/downloads**

2. Click the large yellow **"Download Python"** button.

3. Once the file downloads, open it (double-click).

4. **IMPORTANT:** On the first screen of the installer, check the box that says:
   **"Add Python to PATH"**
   *(This box is near the bottom of the window — do not skip this step.)*

5. Click **"Install Now"** and wait for it to complete.

6. Click **"Close"** when finished.

**To verify Python installed correctly:**

- Press the **Windows key**, type `cmd`, and press Enter.
- In the black window that appears, type: `python --version` and press Enter.
- You should see something like: `Python 3.12.0`
- If you see that, Python is installed. You can close this window.

---

## 3. Step 2 — Install Required Libraries

The program needs a few additional components. This is done with one command.

1. Press the **Windows key**, type `cmd`, and press Enter to open the Command Prompt.

2. Copy and paste the following line exactly, then press **Enter**:

   ```
   pip install pandas reportlab openpyxl pillow
   ```

3. You will see text scrolling — this is normal. Wait until it stops and you see the cursor again.

4. Close the Command Prompt window.

You only need to do this once. These libraries stay installed permanently.

---

## 4. Step 3 — Set Up the Program Files

1. Locate the `SQLite_CMMS-main` folder (from your email or download).

2. Move or copy this folder to somewhere easy to find, for example:
   - Your **Desktop**, or
   - `C:\AIT_CMMS\`

3. Open the folder. Inside you will see another folder called **`AIT_CMMS2.3.1`** — open that too.

4. You should see many files including one called **`AIT_CMMS_REV3.py`** — this is the program.

> **Important:** Do not move individual files out of the `AIT_CMMS2.3.1` folder. All files must stay together for the program to work correctly.

---

## 5. Step 4 — Run the Program

**First time and every time you want to open the program:**

1. Open the `AIT_CMMS2.3.1` folder.
2. Double-click the file named **`AIT_CMMS_REV3.py`**.
3. The program will open within a few seconds.

> **Tip — Create a shortcut:** Right-click `AIT_CMMS_REV3.py` → Send to → Desktop (create shortcut). You can then double-click that shortcut from your Desktop each time.

> **If double-clicking opens Notepad instead of running the program:**
> Right-click the file → Open with → Choose another app → scroll down and select **Python** → check "Always use this app" → OK.

---

## 6. Logging In

When the program opens, a login screen will appear.

### Default User Accounts

| Username  | Password     | Role                |
|-----------|-------------|---------------------|
| `admin`   | `admin123`  | Manager (full access) |
| `manager` | `manager123`| Manager (full access) |
| `tech1`   | `tech1`     | Technician          |
| `apenson` | `apenson`   | Parts Coordinator   |

> **Security recommendation:** Change passwords after first login via the user management settings.

Enter your username and password, then click **Login**.

---

## 7. Navigating the System

Once logged in, the main window has several tabs across the top. Here is what each one does:

| Tab | Purpose |
|-----|---------|
| **Dashboard** | Overview — equipment count, overdue PMs, recent activity |
| **Equipment** | View, add, edit, and manage all equipment/assets |
| **PM Schedule** | Weekly preventive maintenance assignments and schedule |
| **Corrective Maintenance (CM)** | Log and track repair work orders |
| **Parts / MRO** | Spare parts and stock inventory |
| **Manuals** | Equipment documentation and reference files |
| **Reports** | Generate and export monthly summary and other reports |
| **Users** | Manage user accounts (Manager role only) |
| **Backup** | Back up and restore the database |

---

## 8. Common Tasks

### Add New Equipment

1. Click the **Equipment** tab.
2. Click **Add Equipment**.
3. Fill in the fields: BFM Number, SAP Number, Description, Location, Priority.
4. Select which PM types apply (Weekly / Monthly / Six-Month / Annual).
5. Click **Save**.

---

### Record a Completed PM

1. Click the **PM Schedule** tab.
2. Find the equipment in the list.
3. Click **Record Completion** (or right-click the item for options).
4. Enter the technician name, date, hours worked, and any notes.
5. Click **Save**.

---

### Log a Corrective Maintenance Work Order

1. Click the **Corrective Maintenance** tab.
2. Click **New CM**.
3. Enter the equipment, description of the fault, and priority.
4. Click **Save** — a CM number is automatically assigned.
5. Update the status (Open → In Progress → Closed) as work progresses.

---

### Search for Equipment

1. Click the **Equipment** tab.
2. Use the **Search** bar at the top to type a BFM number, description, or location.
3. The list filters as you type.

---

### Change Your Password

1. Click your username or go to the **Users** menu.
2. Select **Change Password**.
3. Enter your current password, then your new password twice.
4. Click **Save**.

---

## 9. Running Reports

### Monthly Summary Report

1. Click the **Reports** tab (or use the menu bar).
2. Select **Monthly PM Summary**.
3. Choose the **Month** and **Year** from the dropdowns.
4. Click **Generate Report** or **Export**.
5. Choose where to save the file — it will be saved as a `.csv` or `.xlsx` file.

### Equipment List Export

1. Click the **Equipment** tab.
2. Click **Export** or **Export to CSV**.
3. Choose a save location.
4. Open the file in Excel.

### Corrective Maintenance Report

1. Click the **Corrective Maintenance** tab.
2. Click **Export Report**.
3. Choose the format: **CSV**, **Excel**, or **PDF**.
4. Choose a save location and click **Save**.

---

## 10. Backing Up Your Data

All your data is stored in a single database file (`.db` file) inside the `AIT_CMMS2.3.1` folder. Back it up regularly to avoid data loss.

### Using the Built-In Backup Tool

1. Click the **Backup** tab.
2. Click **Create Backup**.
3. Choose a destination folder (a USB drive or cloud folder like OneDrive/Google Drive is ideal).
4. Click **Save**.

### Manual Backup (Simple Method)

1. Close the program first.
2. Open the `AIT_CMMS2.3.1` folder.
3. Find the file ending in `.db` — this is your entire database.
4. Copy it to a USB drive, OneDrive, or Google Drive folder.

> **Recommendation:** Back up weekly at minimum. Store at least one copy off-site or in cloud storage.

---

## 11. Troubleshooting

---

**The program doesn't open when I double-click the file**

- Make sure Python is installed (see Step 1).
- Right-click the `.py` file → Open with → Python.
- If Python is not listed, re-install Python and check "Add to PATH".

---

**I see an error message about a missing module**

Open Command Prompt and run:
```
pip install pandas reportlab openpyxl pillow
```
Then try opening the program again.

---

**The program opens but then crashes immediately**

- Make sure all files are still inside the `AIT_CMMS2.3.1` folder — nothing should be moved out.
- Check that the `.db` database file is in the same folder as `AIT_CMMS_REV3.py`.

---

**I forgot my password**

Contact your system administrator to reset it via the **Users** tab (Manager role required).

---

**The program is running slowly**

- Close other programs to free up memory.
- If the equipment list is very large, use the search/filter to narrow results before loading.

---

**I accidentally deleted something**

- Check the Backup tab and restore from the most recent backup.
- This is why regular backups are strongly recommended.

---

## Quick Reference Card

| Task | Where to go |
|------|-------------|
| Open program | Double-click `AIT_CMMS_REV3.py` |
| Add equipment | Equipment tab → Add Equipment |
| Record PM completion | PM Schedule tab → Record Completion |
| Log a repair job | Corrective Maintenance tab → New CM |
| Run monthly report | Reports tab → Monthly PM Summary |
| Export equipment list | Equipment tab → Export |
| Back up data | Backup tab → Create Backup |
| Change password | Users menu → Change Password |

---

*AIT Complete CMMS — Computerized Maintenance Management System*
*For technical support, contact your system administrator.*
