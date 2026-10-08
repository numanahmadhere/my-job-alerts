/**
 * My Job Alerts: Sheets writer
 * Replace your current Apps Script code with this, then:
 * Deploy > Manage deployments > pencil icon > Version: New version > Deploy.
 * The web app URL stays the same, so your GitHub secrets don't change.
 */
const MASTER_ID = 'PASTE_MASTER_SHEET_ID';
const PUBLIC_ID = 'PASTE_PUBLIC_SHEET_ID';

function doPost(e) {
  const lock = LockService.getScriptLock();
  lock.waitLock(60000);
  try {
    const body = JSON.parse(e.postData.contents);
    const secret = PropertiesService.getScriptProperties().getProperty('SECRET');
    if (!secret || body.secret !== secret) return out({ ok: false, error: 'unauthorized' });

    const ss = SpreadsheetApp.openById(body.target === 'public' ? PUBLIC_ID : MASTER_ID);
    let sh = ss.getSheetByName(body.tab) || ss.insertSheet(body.tab);
    const headers = body.headers || [];
    const rows = body.rows || [];

    if (body.mode === 'replace') {
      sh.getRange(1, 1, sh.getMaxRows(), sh.getMaxColumns()).breakApart();
      sh.clear();
      if (body.banner) sh.appendRow([body.banner]);
    }
    if (sh.getLastRow() === 0 || (body.mode === 'replace' && headers.length)) {
      if (headers.length) sh.appendRow(headers);
    }
    if (rows.length) {
      sh.getRange(sh.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
    }
    if (body.first) {
      ss.setActiveSheet(sh);
      ss.moveActiveSheet(1);
    }
    if (body.format) formatSheet(sh, !!body.banner, headers.length);
    return out({ ok: true, written: rows.length });
  } catch (err) {
    return out({ ok: false, error: String(err) });
  } finally {
    lock.releaseLock();
  }
}

function formatSheet(sh, hasBanner, nCols) {
  if (!nCols) return;
  const headerRow = hasBanner ? 2 : 1;
  if (hasBanner) {
    sh.getRange(1, 1, 1, nCols).merge()
      .setFontWeight('bold').setFontSize(12)
      .setBackground('#1A2238').setFontColor('#FFFFFF')
      .setVerticalAlignment('middle');
    sh.setRowHeight(1, 36);
  }
  sh.getRange(headerRow, 1, 1, nCols)
    .setFontWeight('bold').setBackground('#E8EDF7').setFontColor('#1A2238');
  sh.setFrozenRows(headerRow);
  const last = sh.getLastRow();
  if (last > headerRow) {
    const body = sh.getRange(headerRow + 1, 1, last - headerRow, nCols);
    body.setVerticalAlignment('top').setWrap(false);
    sh.getBandings().forEach(b => b.remove());
    body.applyRowBanding(SpreadsheetApp.BandingTheme.LIGHT_GREY, false, false);
  }
  for (let c = 1; c <= nCols; c++) {
    sh.autoResizeColumn(c);
    if (sh.getColumnWidth(c) > 320) sh.setColumnWidth(c, 320);
  }
}

function out(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
