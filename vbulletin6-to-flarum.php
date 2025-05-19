<?php
/**
 * vBulletin 6 to Flarum Migration Script
 *
 * @author Christian Alfredsson
 * @since 1.0.0
 * 
 * Use at own risk
 * 
 */
$script_version = "1.0.0";

set_time_limit(0);
ini_set('memory_limit', '1G');  // Increase memory limit for large forums
ini_set("log_errors", 1);
ini_set("error_log", "vBulletin6_to_Flarum_error.log");

// Progress tracking
$processedUsers = 0;
$processedDiscussions = 0;
$processedPosts = 0;
$batchSize = 1000000; // Process in large batches by default
$offset = 0; // Kör alltid från början om du vill migrera allt
if ($batchSize < 10000) {
    consoleOut("VARNING: batchSize är lågt ($batchSize). Sätt till ett högt värde för att migrera hela forumet i ett svep.");
}

// Lägg till styrning för vilka steg som ska köras
// Exempel: 1 => Gruppmigrering, 2 => Användarmigrering, 3 => Tagg-migrering, 4 => Diskussioner/inlägg, osv
$runSteps = [
    1 => true,  // Gruppmigrering
    2 => true,  // Användarmigreringe
    3 => true,  // Tagg-migrering
    4 => true,  // Diskussioner/inlägg
    5 => true,  // User/discussion-relationer
    6 => true,  // Räkning av diskussioner/kommentarer
    7 => true,  // Tag-sortering
    8 => true,  // Stängning av anslutningar
];

//-----------------------------------------------------------------------------
// vBulletin 6 and Flarum database information (must be same database server)
// Update the following settings to reflect your environment
//
$servername         = "localhost";   // Your database server
$username           = "user in database";   // Your database server username
$password           = "SECRET";   // Your database server password
$vbulletinDbName    = "database";   // Your vBulletin 6 database name  
$vbulletinDbPrefix  = "vb5_";   // Your vBulletin 6 database table prefix (usually vb5_)
$flarumDbName       = "database";   // Your Flarum database name
$flarumDbPrefix     = "flarum_";   // Your Flarum database table prefix

//-----------------------------------------------------------------------------
//
// File path settings
// These paths should point to your vBulletin and Flarum installations
//
$vbulletinBasePath = "/var/www/forum";     // Path to vBulletin installation
$flarumBasePath    = "/var/www/flarum";    // Path to Flarum installation
$vbAttachmentPath  = "/var/www/forum/bifogad_bild"; // vB attachment path (your specific path)
$flarumUploadPath  = $flarumBasePath . "/public/assets/files"; // Flarum upload path

// Migration options for attachments
$copyAttachments = true;  // Set to true to copy files to Flarum, false to keep them in place and link
$attachmentBaseUrl = "/bifogad_bild"; // URL path for attachments if kept in place

// Efter konfiguration och innan migreringssteg:
$fallbackUserId = 1; // Byt till rätt id för din importbot/admin om du vill

// Testläge: migrera endast en specifik tråd (ange nodeid, t.ex. 766829), 0 = migrera alla
$migreraEndastDiscussionId = 0; // 766829 Sätt till 0 för att migrera alla trådar

//=============================================================================
// FUNCTIONS - defined before use
//=============================================================================

// ---------------------------------------------------------------------------
/**
 * Puts information out to the console.
 *
 * @param  string    $consoleText    Text to put out
 * @param  bool      $timeStamp      Whether or not to show a timestamp
 */
function consoleOut($consoleText, $timeStamp=true) {
   $time_stamp = Date('Y-m-d H:i:s');
   $startStr = "\n";

   if ($timeStamp) {
      $startStr .= $time_stamp.": ";
   }
   $endStr = "";
   
   echo $startStr.$consoleText.$endStr;
   flush();
   if (function_exists('ob_flush') && ob_get_level() > 0) ob_flush();
}

// ---------------------------------------------------------------------------
/**
 * Replaces all special chars and blanks with dashes and sets to lower case.
 *
 * @param  string    $text    Text to slugify
 * @return string    Slugified string
 */
function slugify($text) {
	$text = preg_replace('~[^\\pL\d]+~u', '-', $text);
	$text = trim($text, '-');
	$text = iconv('utf-8', 'us-ascii//TRANSLIT', $text);
	$text = strtolower($text);
	$text = preg_replace('~[^-\w]+~', '', $text);

	if (empty($text))
		return 'n-a';

	return $text;
}

// ---------------------------------------------------------------------------
/**
 * Creates a random hex color code
 *
 * @return string    Hex color code
 */
function rand_color() {
   return '#' . str_pad(dechex(mt_rand(0, 0xFFFFFF)), 6, '0', STR_PAD_LEFT);
}

// ---------------------------------------------------------------------------
/**
 * Escapes MySQL query strings.
 *
 * @param string $inp Input string
 * @return string Escaped string
 */
function mysql_escape_mimic($inp) {
	if (is_array($inp)) {
      return array_map(__METHOD__, $inp);
   }

	if (!empty($inp) && is_string($inp)) {
      return str_replace(array('\\', "\0", "\n", "\r", "'", '"', "\x1a"), array('\\\\', '\\0', '\\n', '\\r', "\\'", '\\"', '\\Z'), $inp);
   }
   
   return $inp;
}

// ---------------------------------------------------------------------------
/**
 * Process attachments for a post and replace attachment tags with proper links
 *
 * @param object $vbConnection vBulletin database connection
 * @param object $fConnection Flarum database connection
 * @param int $nodeid Node ID to process attachments for
 * @param string $content Post content
 * @param string $vbPrefix vBulletin table prefix
 * @param string $fPrefix Flarum table prefix
 * @return string Processed content with attachment links
 */
function processAttachments($vbConnection, $fConnection, $nodeid, $content, $vbPrefix, $fPrefix) {
   global $copyAttachments, $attachmentBaseUrl, $attachContentTypeId, $vbAttachmentPath, $flarumUploadPath, $fallbackUserId;
   consoleOut("processAttachments anropad för nodeid $nodeid");
   $relativePaths = [];
   // Hämta alla bilagor till denna post enligt vBulletin 6-modellen
   $attachQuery = $vbConnection->query("
      SELECT a.*, f.*, n.*
      FROM ${vbPrefix}node n
      JOIN ${vbPrefix}attach a ON n.nodeid = a.nodeid
      JOIN ${vbPrefix}filedata f ON a.filedataid = f.filedataid
      WHERE n.parentid = '$nodeid' AND n.contenttypeid = '$attachContentTypeId' AND a.visible = 1
   ");
   $found = 0;
   if ($attachQuery && $attachQuery->num_rows > 0) {
      consoleOut("Hittade ".$attachQuery->num_rows." bilagor för nodeid $nodeid");
      while ($attach = $attachQuery->fetch_assoc()) {
         $found++;
         $filedataid = $attach['filedataid'];
         $filename = isset($attach['filename']) && $attach['filename'] ? $attach['filename'] : ($attach['filedataid'] . '.attach');
         $extension = $attach['extension'];
         $actorId = $attach['userid'] ?: $fallbackUserId;
         // Alltid rekursiv sökning efter bilagan
         $foundPath = false;
         $iterator = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($vbAttachmentPath, RecursiveDirectoryIterator::SKIP_DOTS),
            RecursiveIteratorIterator::LEAVES_ONLY
         );
         foreach ($iterator as $file) {
            if ($file->getFilename() === $filedataid . '.attach') {
               $sourcePath = $file->getPathname();
               $foundPath = true;
               consoleOut("Hittade bilaga via rekursiv sökning: $sourcePath");
               break;
            }
         }
         if (!$foundPath) {
            consoleOut("BILAGA SAKNAS: $filedataid.attach (filedataid=$filedataid, filename=$filename)");
            continue;
         }
         $dateFolder = date('Y-m-d');
         $destDir = $flarumUploadPath . '/' . $dateFolder;
         if (!is_dir($destDir)) {
            if (!mkdir($destDir, 0777, true)) {
               consoleOut("Kunde INTE skapa katalog: $destDir");
               continue;
            } else {
               consoleOut("Skapade katalog: $destDir");
            }
         }
         $destFilename = time() . '-' . mt_rand(100000,999999) . '-' . preg_replace('/[^a-zA-Z0-9._-]/', '', $filename);
         $destPath = $destDir . '/' . $destFilename;
         $relativePath = $dateFolder . '/' . $destFilename;
         // Bygg full URL för url-fältet
         $baseUrl = 'https://forum.ciklid.org'; // Ändra till din domän om det behövs
         $fullUrl = $baseUrl . '/assets/files/' . $relativePath;
         $attachmentUrl = '/assets/files/' . $relativePath;
         // Kopiera filen om den inte redan finns
         if ($copyAttachments && !file_exists($destPath)) {
            if (!copy($sourcePath, $destPath)) {
               consoleOut("Kunde INTE kopiera bilaga: $sourcePath -> $destPath");
               continue;
            } else {
               consoleOut("Bilaga kopierad: $destPath");
            }
         }
         $flarumFileId = null;
         $fileCheck = $fConnection->query("SELECT id FROM ${fPrefix}fof_upload_files WHERE path = '" . $fConnection->real_escape_string($relativePath) . "'");
         if ($fileCheck && $fileCheck->num_rows > 0) {
            $fileData = $fileCheck->fetch_assoc();
            $flarumFileId = $fileData['id'];
            consoleOut("Filen finns redan i fof_upload_files (id=$flarumFileId, path=$relativePath)");
         } else if (file_exists($destPath)) {
            $now = date('Y-m-d H:i:s');
            $mimeType = mime_content_type($destPath);
            $filesize = filesize($destPath);
            $insertSql =
               "INSERT INTO ${fPrefix}fof_upload_files (actor_id, base_name, path, url, type, size, upload_method, created_at) VALUES (" .
               "'" . $actorId . "'," .
               "'" . $fConnection->real_escape_string($filename) . "'," .
               "'" . $fConnection->real_escape_string($relativePath) . "'," .
               "'" . $fConnection->real_escape_string($fullUrl) . "'," .
               "'" . $fConnection->real_escape_string($mimeType) . "'," .
               "'$filesize'," .
               "'local'," .
               "'$now')";
            consoleOut("Försöker INSERT i fof_upload_files: $insertSql");
            $insertFile = $fConnection->query($insertSql);
            if ($insertFile === false) {
               consoleOut("SQL error vid INSERT i fof_upload_files: " . $fConnection->error);
               consoleOut("Misslyckad SQL: $insertSql");
            } else {
               $flarumFileId = $fConnection->insert_id;
               consoleOut("INSERT lyckades i fof_upload_files, id=$flarumFileId");
            }
         }
         // Samla alla relativePaths som ska kopplas till posten
         $relativePaths[] = $relativePath;
         $attachTagPattern = '/\\[ATTACH(?:=CONFIG)?\\]' . $filedataid . '\\[\/ATTACH\\]/i';
         if (preg_match($attachTagPattern, $content)) {
            $content = preg_replace($attachTagPattern, '<img src="' . $fullUrl . '" alt="' . htmlspecialchars($filename) . '" />', $content);
            consoleOut("Ersatte [ATTACH]-tagg för filedataid $filedataid i post $nodeid");
         } else if (strpos($content, $filename) === false && strpos($content, $fullUrl) === false) {
            $content .= '<br><img src="' . $fullUrl . '" alt="' . htmlspecialchars($filename) . '" />';
            consoleOut("Lade till bilaga sist i post $nodeid");
         }
      }
   } else {
      consoleOut("Inga bilagor hittades för post nodeid $nodeid");
   }
   // Hantera bilder som länkas via filedata/fetch?photoid=...
   if (preg_match_all('/filedata\/fetch\?photoid=(\d+)/i', $content, $matches)) {
      foreach ($matches[1] as $photoid) {
         consoleOut("Hittade photoid-bild: $photoid i post $nodeid");
         // 1. Slå upp nodeid = photoid i vb5_node
         $nodeQuery = $vbConnection->query("SELECT * FROM ${vbPrefix}node WHERE nodeid = '$photoid'");
         if ($nodeQuery && $nodeQuery->num_rows > 0) {
            $node = $nodeQuery->fetch_assoc();
            $filedataid = $node['filedataid'];
            if (!$filedataid) {
                consoleOut("PHOTOID-BILD SAKNAS: nodeid $photoid har ingen filedataid");
                continue;
            }
            // 2. Hämta filinfo från filedata
            $fileQuery = $vbConnection->query("SELECT * FROM ${vbPrefix}filedata WHERE filedataid = '$filedataid'");
            if ($fileQuery && $fileQuery->num_rows > 0) {
                $file = $fileQuery->fetch_assoc();
                $filename = isset($file['filename']) && $file['filename'] ? $file['filename'] : ($filedataid . '.' . $file['extension']);
                $extension = $file['extension'];
                // 3. Sök filen på disk (rekursivt)
                $foundPath = false;
                $iterator = new RecursiveIteratorIterator(
                    new RecursiveDirectoryIterator($vbAttachmentPath, RecursiveDirectoryIterator::SKIP_DOTS),
                    RecursiveIteratorIterator::LEAVES_ONLY
                );
                foreach ($iterator as $f) {
                    if ($f->getFilename() === $filedataid . '.attach') {
                        $sourcePath = $f->getPathname();
                        $foundPath = true;
                        consoleOut("Hittade photoid-fil via rekursiv sökning: $sourcePath");
                        break;
                    }
                }
                if (!$foundPath) {
                    consoleOut("PHOTOID-BILD SAKNAS: $filedataid.attach (filedataid=$filedataid, filename=$filename)");
                    continue;
                }
                $dateFolder = date('Y-m-d');
                $destDir = $flarumUploadPath . '/' . $dateFolder;
                if (!is_dir($destDir)) {
                    if (!mkdir($destDir, 0777, true)) {
                        consoleOut("Kunde INTE skapa katalog: $destDir");
                        continue;
                    } else {
                        consoleOut("Skapade katalog: $destDir");
                    }
                }
                $destFilename = time() . '-' . mt_rand(100000,999999) . '-' . preg_replace('/[^a-zA-Z0-9._-]/', '', $filename);
                $destPath = $destDir . '/' . $destFilename;
                $relativePath = $dateFolder . '/' . $destFilename;
                $baseUrl = 'https://forum.ciklid.org';
                $fullUrl = $baseUrl . '/assets/files/' . $relativePath;
                // Kopiera filen om den inte redan finns
                if ($copyAttachments && !file_exists($destPath)) {
                    if (!copy($sourcePath, $destPath)) {
                        consoleOut("Kunde INTE kopiera photoid-bild: $sourcePath -> $destPath");
                        continue;
                    } else {
                        consoleOut("Photoid-bild kopierad: $destPath");
                    }
                }
                // Skapa/leta upp fof_upload_files
                $flarumFileId = null;
                $fileCheck = $fConnection->query("SELECT id FROM ${fPrefix}fof_upload_files WHERE path = '" . $fConnection->real_escape_string($relativePath) . "'");
                if ($fileCheck && $fileCheck->num_rows > 0) {
                    $fileData = $fileCheck->fetch_assoc();
                    $flarumFileId = $fileData['id'];
                    consoleOut("Photoid-bild finns redan i fof_upload_files (id=$flarumFileId, path=$relativePath)");
                } else if (file_exists($destPath)) {
                    $now = date('Y-m-d H:i:s');
                    $mimeType = mime_content_type($destPath);
                    $filesize = filesize($destPath);
                    $insertSql =
                        "INSERT INTO ${fPrefix}fof_upload_files (actor_id, base_name, path, url, type, size, upload_method, created_at) VALUES (" .
                        "'" . $fallbackUserId . "'," .
                        "'" . $fConnection->real_escape_string($filename) . "'," .
                        "'" . $fConnection->real_escape_string($relativePath) . "'," .
                        "'" . $fConnection->real_escape_string($fullUrl) . "'," .
                        "'" . $fConnection->real_escape_string($mimeType) . "'," .
                        "'$filesize'," .
                        "'local'," .
                        "'$now')";
                    consoleOut("Försöker INSERT i fof_upload_files (photoid): $insertSql");
                    $insertFile = $fConnection->query($insertSql);
                    if ($insertFile === false) {
                        consoleOut("SQL error vid INSERT i fof_upload_files (photoid): " . $fConnection->error);
                        consoleOut("Misslyckad SQL: $insertSql");
                    } else {
                        $flarumFileId = $fConnection->insert_id;
                        consoleOut("INSERT lyckades i fof_upload_files (photoid), id=$flarumFileId");
                    }
                }
                // Byt ut ALLA länkar (med eller utan extra parametrar) i $content
                $content = preg_replace(
                    '/filedata\/fetch\?photoid=' . $photoid . '[^"\]\s]*/i',
                    $fullUrl,
                    $content
                );
            } else {
                consoleOut("PHOTOID-BILD SAKNAS I FILEDATA: $filedataid");
            }
        } else {
            consoleOut("PHOTOID-BILD SAKNAS I NODE: $photoid");
        }
      }
   }
   return [$content, $relativePaths];
}

// ---------------------------------------------------------------------------
/**
 * Generate a Flarum-compatible filename for an attachment
 *
 * @param int $filedataid vBulletin file data ID
 * @param string $extension File extension
 * @return string Generated filename
 */
function generateFlarumFilename($filedataid, $extension) {
   // Create a unique filename based on the file data ID
   $hash = md5($filedataid . time());
   return $hash . '.' . $extension;
}

// ---------------------------------------------------------------------------
/**
 * Find vBulletin attachment file in filesystem
 *
 * @param int $filedataid File data ID
 * @param string $basePath Base attachment path
 * @return string|false Full path to file or false if not found
 */
function findVBAttachmentFile($filedataid, $basePath) {
   // vBulletin typically stores files in subdirectories based on ID
   // Common patterns: /attachments/1/2/3/123.attach or similar
   
   // Try direct filename patterns
   $patterns = [
      $basePath . '/' . $filedataid . '.attach',
      $basePath . '/' . $filedataid,
      $basePath . '/' . floor($filedataid / 1000) . '/' . $filedataid . '.attach',
      $basePath . '/' . floor($filedataid / 1000) . '/' . $filedataid,
   ];
   
   foreach ($patterns as $pattern) {
      if (file_exists($pattern)) {
         return $pattern;
      }
   }
   
   // If not found, search recursively (disabled for large file systems)
   /*
   $iterator = new RecursiveIteratorIterator(
      new RecursiveDirectoryIterator($basePath, RecursiveDirectoryIterator::SKIP_DOTS),
      RecursiveIteratorIterator::LEAVES_ONLY
   );
   
   foreach ($iterator as $file) {
      if (strpos($file->getFilename(), $filedataid) !== false) {
         return $file->getPathname();
      }
   }
   */
   
   return false;
}

// ---------------------------------------------------------------------------
/**
 * Get MIME type for file extension
 *
 * @param string $extension File extension
 * @return string MIME type
 */
function getMimeType($extension) {
   $mimeTypes = [
      'jpg' => 'image/jpeg',
      'jpeg' => 'image/jpeg',
      'png' => 'image/png',
      'gif' => 'image/gif',
      'webp' => 'image/webp',
      'pdf' => 'application/pdf',
      'doc' => 'application/msword',
      'docx' => 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
      'txt' => 'text/plain',
      'zip' => 'application/zip',
      'rar' => 'application/x-rar-compressed',
   ];
   
   $extension = strtolower($extension);
   return isset($mimeTypes[$extension]) ? $mimeTypes[$extension] : 'application/octet-stream';
}

// ---------------------------------------------------------------------------
/**
 * Create a file record in Flarum's FoF Upload tables
 *
 * @param object $fConnection Flarum database connection
 * @param string $fPrefix Flarum table prefix
 * @param int $nodeid Node ID for linking
 * @param string $filename Original filename
 * @param string $newFilename New filename
 * @param string $extension File extension
 * @param int $filesize File size
 * @return int|false Insert ID or false on failure
 */
function createFlarumFileRecord($fConnection, $fPrefix, $nodeid, $filename, $newFilename, $extension, $filesize) {
   $now = date('Y-m-d H:i:s');
   $mimeType = getMimeType($extension);
   
   $query = "INSERT INTO " . $fPrefix . "fof_upload_files (
      base_name,
      path,
      url,
      type,
      size,
      upload_method,
      created_at
   ) VALUES (
      '" . $fConnection->real_escape_string($filename) . "',
      '" . $fConnection->real_escape_string($newFilename) . "',
      '/assets/files/" . $newFilename . "',
      '" . $fConnection->real_escape_string($mimeType) . "',
      '$filesize',
      'local',
      '$now'
   )";
   
   $res = $fConnection->query($query);
   if ($res) {
      $flarumFileId = $fConnection->insert_id;
      
      // Link file to post if post exists
      $postCheck = $fConnection->query("SELECT id FROM " . $fPrefix . "posts WHERE id = '$nodeid'");
      if ($postCheck && $postCheck->num_rows > 0) {
         $linkQuery = "INSERT INTO " . $fPrefix . "fof_upload_file_posts (file_id, post_id) VALUES ('$flarumFileId', '$nodeid')";
         $fConnection->query($linkQuery);
      }
      
      return $flarumFileId;
   }
   
   return false;
}

// ---------------------------------------------------------------------------
/**
 * Check if a table exists in the database
 *
 * @param object $connection Database connection
 * @param string $tableName Table name to check
 * @return bool True if table exists
 */
function table_exists($connection, $tableName) {
   $result = $connection->query("SHOW TABLES LIKE '$tableName'");
   return $result && $result->num_rows > 0;
}

// ---------------------------------------------------------------------------
/**
 * Get vBulletin attachment path pattern for file ID
 *
 * @param int $filedataid File data ID
 * @return string Relative path pattern
 */
function getVBAttachmentPath($filedataid) {
   // This would need to match your actual vBulletin storage pattern
   // Common patterns are subdirectories based on ID
   return floor($filedataid / 1000) . '/' . $filedataid . '.attach';
}

// ---------------------------------------------------------------------------
/**
 * Formats vBulletin's text to Flarum's text format.
 *
 * @param  string    $text          Text to convert
 * @return string    Converted text
 */
function formatTextForFlarum($text) {
   global $flarumDbConnection;
   if (empty($text)) {
      return '';
   }
   // 1. Hantera bokstavliga \n och \r\n till riktiga radbrytningar
   $text = str_replace(["\\r\\n", "\\n", "\\r"], "\n", $text);
   // 2. Hantera riktiga CRLF och CR till LF
   $text = str_replace(["\r\n", "\r"], "\n", $text);
   // 3. Konvertera alla radbrytningar till \n (säkerställer enhetlighet)
   $text = preg_replace("/(\r\n|\r|\n)/", "\n", $text);
   // Konvertera BBCode till HTML
   $text = convertBBCodeToHTML($text);
   // Ta bort kvarvarande BBCode-taggar
   $text = preg_replace('/\[.*?\]/', '', $text);
   // Om texten redan innehåller HTML-taggar, escapa inte igen
   $hasHtml = preg_match('/<[^>]+>/', $text);
   // Konvertera radbrytningar till <br>
   $text = nl2br($text);
   // Rensa HTML entities
   $text = html_entity_decode($text, ENT_QUOTES, 'UTF-8');
   // Trimma
   $text = trim($text);
   return $flarumDbConnection->real_escape_string($text);
}

// ---------------------------------------------------------------------------
/**
 * Converts basic BBcode to HTML.
 *
 * @param  string    $bbcode    Text to convert
 * @return string    Converted text
 */
function convertBBCodeToHTML($bbcode) {
   if (empty($bbcode)) {
      return '';
   }

   // Bold
   $bbcode = preg_replace('/\[b\](.*?)\[\/b\]/is', '<strong>$1</strong>', $bbcode);
   
   // Italic
   $bbcode = preg_replace('/\[i\](.*?)\[\/i\]/is', '<em>$1</em>', $bbcode);
   
   // Underline
   $bbcode = preg_replace('/\[u\](.*?)\[\/u\]/is', '<u>$1</u>', $bbcode);
   
   // Links with custom text
   $bbcode = preg_replace('/\[url=(.*?)\](.*?)\[\/url\]/is', '<a href="$1">$2</a>', $bbcode);
   
   // Simple links
   $bbcode = preg_replace('/\[url\](.*?)\[\/url\]/is', '<a href="$1">$1</a>', $bbcode);
   
   // Images
   $bbcode = preg_replace('/\[img\](.*?)\[\/img\]/is', '<img src="$1" alt="" />', $bbcode);
   
   // Quotes
   $bbcode = preg_replace('/\[quote\](.*?)\[\/quote\]/is', '<blockquote>$1</blockquote>', $bbcode);
   $bbcode = preg_replace('/\[quote=(.*?)\](.*?)\[\/quote\]/is', '<blockquote><cite>$1</cite>$2</blockquote>', $bbcode);
   
   // Code blocks
   $bbcode = preg_replace('/\[code\](.*?)\[\/code\]/is', '<pre><code>$1</code></pre>', $bbcode);
   
   // Lists
   $bbcode = preg_replace('/\[list\](.*?)\[\/list\]/is', '<ul>$1</ul>', $bbcode);
   $bbcode = preg_replace('/\[list=1\](.*?)\[\/list\]/is', '<ol>$1</ol>', $bbcode);
   $bbcode = preg_replace('/\[\*\](.*?)(?=\[\*\]|\[\/list\])/is', '<li>$1</li>', $bbcode);
   
   // Size tags - convert to appropriate HTML
   $bbcode = preg_replace('/\[size=([0-9]+)\](.*?)\[\/size\]/is', '<span style="font-size:$1px">$2</span>', $bbcode);
   
   // Color tags
   $bbcode = preg_replace('/\[color=(.*?)\](.*?)\[\/color\]/is', '<span style="color:$1">$2</span>', $bbcode);

   return $bbcode;
}

// Lägg till hjälpfunktion för translitterering och unikt användarnamn
function makeFlarumUsername($rawUsername, $flarumDbConnection, $flarumDbPrefix) {
    // 1. Translitterera åäö
    $username = strtr($rawUsername, [
        'å' => 'a', 'ä' => 'a', 'ö' => 'o',
        'Å' => 'A', 'Ä' => 'A', 'Ö' => 'O',
    ]);
    // 2. Ersätt mellanslag med bindestreck
    $username = str_replace(' ', '-', $username);
    // 3. Ta bort otillåtna tecken (endast a-z, A-Z, 0-9, - och _)
    $username = preg_replace('/[^a-zA-Z0-9\-_]/', '', $username);
    // 4. Behåll original-casing (Flarum är case-insensitive men tillåter versaler)
    $base = $username;
    $username = $base;
    $suffix = 0;
    // 5. Kolla om namnet redan finns i Flarum
    while (true) {
        $check = $flarumDbConnection->query("SELECT id FROM " . $flarumDbPrefix . "users WHERE username = '" . $flarumDbConnection->real_escape_string($username) . "'");
        if ($check && $check->num_rows == 0) break;
        $suffix++;
        $username = $base . $suffix;
    }
    return $username;
}

// Lägg till hjälpfunktion för att koppla bilagor till post efter att posten är skapad
function linkAttachmentsToPost($fConnection, $fPrefix, $postId, $relativePaths) {
    foreach ($relativePaths as $relativePath) {
        $fileCheck = $fConnection->query("SELECT id FROM ${fPrefix}fof_upload_files WHERE path = '" . $fConnection->real_escape_string($relativePath) . "'");
        if ($fileCheck && $fileCheck->num_rows > 0) {
            $fileData = $fileCheck->fetch_assoc();
            $flarumFileId = $fileData['id'];
            $postExistsCheck = $fConnection->query("SELECT id FROM ${fPrefix}posts WHERE id = '$postId'");
            if ($flarumFileId && $postExistsCheck && $postExistsCheck->num_rows > 0) {
                $filePostCheck = $fConnection->query("SELECT * FROM ${fPrefix}fof_upload_file_posts WHERE file_id = '$flarumFileId' AND post_id = '$postId'");
                if (!$filePostCheck || $filePostCheck->num_rows == 0) {
                    $fConnection->query("INSERT INTO ${fPrefix}fof_upload_file_posts (file_id, post_id) VALUES ('$flarumFileId', '$postId')");
                }
            }
        }
    }
}

//----------------------------------------------------------------------------- 
// Öppna databasanslutningar
//-----------------------------------------------------------------------------

consoleOut("\n===============================================================================", false);
consoleOut("vBulletin 6 to Flarum Migration Script                                 v".$script_version, false);

$step = 0;
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: OPENING DATABASE CONNECTIONS\n", false);

$vbulletinDbConnection = new mysqli($servername, $username, $password, $vbulletinDbName);
if ($vbulletinDbConnection->connect_error) {
   consoleOut("Connection to vBulletin database failed: ".$vbulletinDbConnection->connect_error);
   die("Script stopped");
} else {
   consoleOut("Connection to vBulletin database successful");
   if(!$vbulletinDbConnection->set_charset("utf8mb4")) {
      consoleOut("Error loading character set utf8mb4: ".$vbulletinDbConnection->error);
      exit();
   } else {
      consoleOut("Current character set: ".$vbulletinDbConnection->character_set_name());
   }
}

$flarumDbConnection = new mysqli($servername, $username, $password, $flarumDbName);
if ($flarumDbConnection->connect_error) {
   consoleOut("Connection to Flarum database failed: ".$flarumDbConnection->connect_error);
   die("Script stopped");
} else {
   consoleOut("Connection to Flarum database successful");
   if(!$flarumDbConnection->set_charset("utf8mb4")) {
      consoleOut("Error loading character set utf8mb4: ".$flarumDbConnection->error);
      exit();
   } else {
      consoleOut("Current character set: ".$flarumDbConnection->character_set_name());
   }
}

// Efter att vBulletin-databasen är ansluten, hämta contenttypeid för Channel, Text och Attach
$channelContentTypeId = null;
$textContentTypeId = null;
$attachContentTypeId = null;
$res = $vbulletinDbConnection->query("SELECT contenttypeid, class FROM ${vbulletinDbPrefix}contenttype");
if ($res) {
    while ($row = $res->fetch_assoc()) {
        if ($row['class'] === 'Channel') $channelContentTypeId = $row['contenttypeid'];
        if ($row['class'] === 'Text') $textContentTypeId = $row['contenttypeid'];
        if ($row['class'] === 'Attach') $attachContentTypeId = $row['contenttypeid'];
    }
}
if (!$channelContentTypeId || !$textContentTypeId || !$attachContentTypeId) {
    consoleOut("Kunde inte hitta contenttypeid för Channel/Text/Attach. Kontrollera tabellen contenttype.");
    die("Script stopped");
}
consoleOut("contenttypeid: Channel=$channelContentTypeId, Text=$textContentTypeId, Attach=$attachContentTypeId");

// Hämta alla contenttypeid för trådstarter (Text, Starter, Discussion etc)
$discussionContentTypeIds = [];
$res = $vbulletinDbConnection->query("SELECT contenttypeid, class FROM ${vbulletinDbPrefix}contenttype");
if ($res) {
    while ($row = $res->fetch_assoc()) {
        if (in_array($row['class'], ['Text', 'Starter', 'Discussion'])) {
            $discussionContentTypeIds[] = $row['contenttypeid'];
        }
    }
}
if (empty($discussionContentTypeIds)) {
    consoleOut("Kunde inte hitta contenttypeid för Text/Starter/Discussion. Kontrollera tabellen contenttype.");
    die("Script stopped");
}
consoleOut("contenttypeid för diskussioner: " . implode(',', $discussionContentTypeIds));

// Efter att databasanslutningen till Flarum är öppnad:
$existingPostIds = [];
$res = $flarumDbConnection->query("SELECT id FROM ${flarumDbPrefix}posts");
if ($res) {
    while ($row = $res->fetch_assoc()) {
        $existingPostIds[$row['id']] = true;
    }
}
consoleOut("Laddade " . count($existingPostIds) . " post-id från Flarum till minnet.");

// Ladda ALLA befintliga diskussions-id:n från Flarum till minnet EN gång, direkt efter anslutning
$existingDiscussionIds = [];
$res = $flarumDbConnection->query("SELECT id FROM ${flarumDbPrefix}discussions");
if ($res) {
    while ($row = $res->fetch_assoc()) {
        $existingDiscussionIds[$row['id']] = true;
    }
}
consoleOut("Laddade " . count($existingDiscussionIds) . " diskussions-id från Flarum till minnet.");

// Ladda om $existingUserIds efter migrering
$existingUserIds = [];
$res = $flarumDbConnection->query("SELECT id FROM ${flarumDbPrefix}users");
if ($res) {
    while ($row = $res->fetch_assoc()) {
        $existingUserIds[(string)$row['id']] = true;
    }
}
consoleOut("Laddade " . count($existingUserIds) . " användar-id från Flarum till minnet.");
consoleOut("DEBUG: Alla user-nycklar i \\$existingUserIds: " . implode(',', array_map('strval', array_keys($existingUserIds))));

// Ladda alla befintliga tagg-id:n till array för snabbare kontroll (om inte redan gjort)
if (!isset($existingTagIds)) {
    $existingTagIds = [];
    $res = $flarumDbConnection->query("SELECT id FROM ${flarumDbPrefix}tags");
    if ($res) {
        while ($row = $res->fetch_assoc()) {
            $existingTagIds[$row['id']] = true;
        }
    }
    consoleOut("Laddade " . count($existingTagIds) . " tagg-id från Flarum till minnet.");
}

// Ladda alla befintliga discussion-slugs till array
$existingDiscussionSlugs = [];
$res = $flarumDbConnection->query("SELECT slug FROM ${flarumDbPrefix}discussions");
if ($res) {
    while ($row = $res->fetch_assoc()) {
        $existingDiscussionSlugs[$row['slug']] = true;
    }
}
consoleOut("Laddade " . count($existingDiscussionSlugs) . " discussion-slugs från Flarum till minnet.");

// Ladda alla befintliga tag-slugs till array
$existingTagSlugs = [];
$res = $flarumDbConnection->query("SELECT slug FROM ${flarumDbPrefix}tags");
if ($res) {
    while ($row = $res->fetch_assoc()) {
        $existingTagSlugs[$row['slug']] = true;
    }
}
consoleOut("Laddade " . count($existingTagSlugs) . " tag-slugs från Flarum till minnet.");

// Logga antal trådar och inlägg i vBulletin och Flarum i början
$vbulletinThreadCount = 0;
$vbulletinPostCount = 0;
$flarumDiscussionCount = 0;
$flarumPostCount = 0;
$res = null;
$res = $vbulletinDbConnection->query("SELECT COUNT(*) AS cnt FROM ${vbulletinDbPrefix}node WHERE contenttypeid = 36 AND parentid > 0");
if ($res) { $row = $res->fetch_assoc(); $vbulletinThreadCount = $row['cnt']; }
$res = $vbulletinDbConnection->query("SELECT COUNT(*) AS cnt FROM ${vbulletinDbPrefix}node WHERE contenttypeid = 36");
if ($res) { $row = $res->fetch_assoc(); $vbulletinPostCount = $row['cnt']; }
$res = $flarumDbConnection->query("SELECT COUNT(*) AS cnt FROM ${flarumDbPrefix}discussions");
if ($res) { $row = $res->fetch_assoc(); $flarumDiscussionCount = $row['cnt']; }
$res = $flarumDbConnection->query("SELECT COUNT(*) AS cnt FROM ${flarumDbPrefix}posts");
if ($res) { $row = $res->fetch_assoc(); $flarumPostCount = $row['cnt']; }
consoleOut("Antal trådstartare i vBulletin: $vbulletinThreadCount");
consoleOut("Antal inlägg i vBulletin: $vbulletinPostCount");
consoleOut("Antal diskussioner i Flarum: $flarumDiscussionCount");
consoleOut("Antal inlägg i Flarum: $flarumPostCount");

//----------------------------------------------------------------------------- 
// Gruppmigrering
//----------------------------------------------------------------------------- 
$step++;
if (!empty($runSteps[1])) {
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: GROUP MIGRATION\n", false);

$result = $vbulletinDbConnection->query("SELECT usergroupid, title, usertitle FROM ${vbulletinDbPrefix}usergroup");
if ($result === false) {
    consoleOut("SQL Error: " . $vbulletinDbConnection->error);
    die("Script stopped");
}
$totalGroups = $result->num_rows;
consoleOut("Hittade $totalGroups grupper i vBulletin.");

if ($totalGroups) {
    $migrated = 0;
    while ($row = $result->fetch_assoc()) {
        $id = $row['usergroupid'];
        consoleOut("Behandlar grupp $id (".$row['title'].") ...");
        consoleOut("usertitle (hex): " . bin2hex($row['usertitle']));
        consoleOut("title (hex): " . bin2hex($row['title']));
        // Flarum har fyra defaultgrupper (ID 1-4), migrera bara ID > 7
        if ($id > 7) {
            // Kontrollera om gruppen redan finns
            $checkGroup = $flarumDbConnection->query("SELECT id FROM ".$flarumDbPrefix."groups WHERE id = '$id'");
            if ($checkGroup && $checkGroup->num_rows > 0) {
                consoleOut("Grupp $id finns redan, hoppar över.");
                continue;
            }
            $name_singular = $row["usertitle"];
            $name_plural = $row["title"];
            $color = rand_color();
            consoleOut("Försöker INSERT för grupp $id: singular='$name_singular', plural='$name_plural', color='$color'");
            try {
                $query = "INSERT INTO ".$flarumDbPrefix."groups (id, name_singular, name_plural, color) VALUES ( '$id', '$name_singular', '$name_plural', '$color')";
                $res = $flarumDbConnection->query($query);
            } catch (Throwable $e) {
                consoleOut("FATAL PHP ERROR: " . $e->getMessage());
                die("Script stopped");
            }
            if ($res === false) {
                consoleOut("SQL error vid grupp $id");
                consoleOut($query, false);
                consoleOut($flarumDbConnection->error."\n", false);
            } else {
                $migrated++;
                if ($migrated % 10 == 0) echo ".";
            }
        }
    }
    consoleOut("\n$migrated grupper migrerade till Flarum.");
} else {
    consoleOut("Inga vBulletin-grupper hittades.");
}
}

//----------------------------------------------------------------------------- 
// Användarmigrering
//----------------------------------------------------------------------------- 
$step++;
if (!empty($runSteps[2])) {
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: USER MIGRATION (modern mappning)\n", false);

$result = $vbulletinDbConnection->query("
   SELECT 
      userid, 
      usergroupid, 
      FROM_UNIXTIME(joindate) as user_joindate, 
      FROM_UNIXTIME(lastactivity) as user_lastactivity, 
      username, 
      email
   FROM ${vbulletinDbPrefix}user 
   WHERE userid > 0
");
if ($result === false) {
    consoleOut("SQL Error: " . $vbulletinDbConnection->error);
    die("Script stopped");
}
$totalUsers = $result->num_rows;
consoleOut("Hittade $totalUsers användare i vBulletin.");

if ($totalUsers) {
    $i = 0;
    $usersIgnored = 0;
    $fakeEmailCounter = 1;
    while ($row = $result->fetch_assoc()) {
        $i++;
        $id = $row['userid'];
        $username = makeFlarumUsername($row["username"], $flarumDbConnection, $flarumDbPrefix);
        $username = $vbulletinDbConnection->real_escape_string($username);
        $displayname = $username;
        $email = $row['email'];
        if ($email == NULL || empty($email)) {
            $email = sprintf("imported%05d@ciklid.org", $fakeEmailCounter);
            $fakeEmailCounter++;
        }
        $email = $vbulletinDbConnection->real_escape_string($email);
        $password = password_hash('BytMig123', PASSWORD_DEFAULT); // Temporärt lösenord
        $joined_at = $row['user_joindate'] ?: date('Y-m-d H:i:s');
        $last_seen_at = $row['user_lastactivity'] ?: $joined_at;
        $checkUser = $flarumDbConnection->query("SELECT id FROM ".$flarumDbPrefix."users WHERE id = '$id'");
        if ($checkUser && $checkUser->num_rows > 0) {
            continue;
        }
        $query = "INSERT INTO ".$flarumDbPrefix."users (id, username, nickname, email, password, joined_at, last_seen_at, is_email_confirmed) VALUES ( '$id', '$username', '$displayname', '$email', '$password', '$joined_at', '$last_seen_at', 1)";
        $res = $flarumDbConnection->query($query);
        if ($res === false) {
            consoleOut("SQL error för användare $id");
            consoleOut($query, false);
            consoleOut($flarumDbConnection->error."\n", false);
        }
    }
    consoleOut(($i-$usersIgnored)." av $totalUsers användare migrerade.");
} else {
    consoleOut("Inga vBulletin-användare hittades.");
}
}

//----------------------------------------------------------------------------- 
// Forums => Tags migration
//----------------------------------------------------------------------------- 
$step++;
if (!empty($runSteps[3])) {
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: FORUMS => TAGS MIGRATION (modern mappning)\n", false);

$result = $vbulletinDbConnection->query("
   SELECT 
      n.nodeid, 
      n.title, 
      n.description,
      n.displayorder,
      n.parentid
   FROM ${vbulletinDbPrefix}node n
   INNER JOIN ${vbulletinDbPrefix}channel c ON n.nodeid = c.nodeid
   WHERE n.contenttypeid = '$channelContentTypeId'
   ORDER BY n.displayorder
");
if ($result === false) {
    consoleOut("SQL Error: " . $vbulletinDbConnection->error);
    die("Script stopped");
}
$totalCategories = $result->num_rows;
consoleOut("Hittade $totalCategories forum (kanaler) i vBulletin.");

if ($totalCategories) {
    $i = 1;
    $migrated = 0;
    while ($row = $result->fetch_assoc()) {
        $id = $row["nodeid"];
        // Dubblettkontroll med PHP-array
        if (isset($existingTagIds[$id])) {
            consoleOut("SKIP: Tagg $id finns redan i Flarum (cache).");
            $i++;
            continue;
        }
        $name = mysql_escape_mimic($row["title"]);
        $slugBase = mysql_escape_mimic(slugify($row["title"]));
        $slug = $slugBase;
        $slugSuffix = 1;
        while (isset($existingTagSlugs[$slug])) {
            $slug = $slugBase . '-' . $slugSuffix;
            $slugSuffix++;
        }
        $existingTagSlugs[$slug] = true;
        $description = mysql_escape_mimic(strip_tags($row["description"]));
        $color = rand_color();
        $position = $row['displayorder'] ?: $i;
        $parent_id = $row['parentid'] ?: 'NULL';
        // Kontrollera att parent_id finns i Flarum, annars sätt till NULL
        if ($parent_id !== 'NULL' && $parent_id > 0) {
            if (!isset($existingTagIds[$parent_id])) {
                $parent_id = 'NULL';
            }
        }
        $query = "INSERT INTO ".$flarumDbPrefix."tags (id, name, description, slug, color, position, parent_id) VALUES ( '$id', '$name', '$description', '$slug', '$color', $position, ".($parent_id === 'NULL' ? 'NULL' : "'$parent_id'").")";
        $res = $flarumDbConnection->query($query);
        if($res === false) {
                consoleOut("SQL error vid tagg $id");
                consoleOut($query, false);
                consoleOut($flarumDbConnection->error."\n", false);
            } else {
                $migrated++;
                $existingTagIds[$id] = true;
            }
        $i++;
    }
    consoleOut("\n$migrated forum migrerade som taggar till Flarum.");
} else {
    consoleOut("Inga vBulletin-forum hittades.");
}
}

//----------------------------------------------------------------------------- 
// Node content => Discussions/Posts Migration
//----------------------------------------------------------------------------- 
$step++;
if (!empty($runSteps[4])) {
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: NODE CONTENT => DISCUSSION/POSTS MIGRATION (modern mappning)\n", false);

// I diskussionsmigreringen:
if ($migreraEndastDiscussionId > 0) {
    $discussionWhere = "n.nodeid = $migreraEndastDiscussionId";
} else {
    $discussionWhere = "n.contenttypeid IN (" . implode(',', $discussionContentTypeIds) . ") AND n.parentid > 0";
}
$questionQuery = $vbulletinDbConnection->query("
   SELECT 
      n.nodeid,
      n.parentid,
      n.userid,
      n.title,
      n.publishdate,
      n.open,
      n.sticky
   FROM ${vbulletinDbPrefix}node n
   WHERE $discussionWhere
   ORDER BY n.nodeid ASC
   LIMIT $batchSize OFFSET $offset
");
consoleOut("Kör batch: LIMIT $batchSize OFFSET $offset. Ändra $offset för nästa körning om du vill fortsätta batchvis.");
if ($questionQuery === false) {
    consoleOut("SQL Error: " . $vbulletinDbConnection->error);
    die("Script stopped");
}
$questionCount = $questionQuery->num_rows;
consoleOut("Hittade $questionCount trådar i vBulletin.");

if ($questionCount) {
    $curTopicCount = 0;
    $migratedDiscussions = 0;
    $skippedDiscussions = 0;
    while($question = $questionQuery->fetch_assoc()) {
        $curTopicCount++;
        $discussionId = $question["nodeid"];
        // Dubblettkontroll med PHP-array
        if (isset($existingDiscussionIds[$discussionId])) {
            consoleOut("SKIP: Diskussion $discussionId finns redan i Flarum (cache).");
            $skippedDiscussions++;
            continue;
        }
        $title = $vbulletinDbConnection->real_escape_string($question["title"]);
        $slugBase = mysql_escape_mimic(slugify($question["title"]));
        $slug = $slugBase;
        $slugSuffix = 1;
        while (isset($existingDiscussionSlugs[$slug])) {
            $slug = $slugBase . '-' . $slugSuffix;
            $slugSuffix++;
        }
        $existingDiscussionSlugs[$slug] = true;
        $parentId = $question["parentid"];
        $createdAt = date('Y-m-d H:i:s', $question['publishdate']);
        $userId = $question["userid"];
        if (empty($userId)) {
            $userId = $fallbackUserId;
        } else {
            $checkUser = $flarumDbConnection->query("SELECT id FROM {$flarumDbPrefix}users WHERE id = '" . intval($userId) . "'");
            if (!$checkUser || $checkUser->num_rows == 0) {
                consoleOut("VARNING: Diskussion $discussionId - user_id $userId finns inte i Flarum. Kopplar till fallback-user $fallbackUserId.");
                $userId = $fallbackUserId;
            }
        }
        $is_locked = ($question["open"] == 0) ? 1 : 0;
        $is_sticky = $question["sticky"] ? 1 : 0;
        $is_approved = 1;
        // Skapa diskussionen
        $query = "INSERT INTO ".$flarumDbPrefix."discussions (
            id, title, slug, created_at, user_id, is_locked, is_sticky, is_approved
        ) VALUES (
            '$discussionId', '$title', '$slug', '$createdAt', '$userId', $is_locked, $is_sticky, $is_approved
        )";
        $res = $flarumDbConnection->query($query);
        if ($res === false) {
            consoleOut("SQL error creating discussion $discussionId");
            consoleOut($query, false);
            consoleOut($flarumDbConnection->error."\n", false);
            continue;
        }
        $migratedDiscussions++;
        $existingDiscussionIds[$discussionId] = true;
        // Koppla till tagg
        $checkTag = $flarumDbConnection->query("SELECT id FROM ".$flarumDbPrefix."tags WHERE id = '$parentId'");
        $fallbackTagId = 1; // Byt till rätt id för din fallback-tagg
        if ($checkTag && $checkTag->num_rows > 0) {
            $query = "INSERT IGNORE INTO ".$flarumDbPrefix."discussion_tag (discussion_id, tag_id) VALUES( '$discussionId', '$parentId')";
            $flarumDbConnection->query($query);
        } else {
            consoleOut("VARNING: Diskussion $discussionId - parentid $parentId finns inte som tagg. Kopplar till fallback-tagg $fallbackTagId.");
            $query = "INSERT IGNORE INTO ".$flarumDbPrefix."discussion_tag (discussion_id, tag_id) VALUES( '$discussionId', '$fallbackTagId')";
            $flarumDbConnection->query($query);
        }
        // Hämta alla inlägg (inklusive trådstart)
        $postsQuery = $vbulletinDbConnection->query("
            SELECT n.nodeid, n.parentid, n.userid, n.publishdate, t.rawtext, t.pagetext
            FROM ${vbulletinDbPrefix}node n
            LEFT JOIN ${vbulletinDbPrefix}text t ON n.nodeid = t.nodeid
            WHERE (n.nodeid = '$discussionId' OR (n.contenttypeid = $textContentTypeId AND n.parentid = '$discussionId'))
            ORDER BY n.publishdate ASC, n.nodeid ASC
        ");
        $postCount = $postsQuery->num_rows;
        consoleOut("Tråd $discussionId: $postCount inlägg hittades i vBulletin.");
        $migratedPosts = 0;
        $skippedPosts = 0;
        $postNumber = 1;
        while($post = $postsQuery->fetch_assoc()) {
            $postId = $post["nodeid"];
            if (isset($existingPostIds[$postId])) {
                consoleOut("SKIP: Post $postId finns redan i Flarum (cache). (Ingen INSERT)");
                $skippedPosts++;
                $postNumber++;
                continue;
            }
            consoleOut("DEBUG: Försöker INSERT för post $postId");
            // Sätt alltid discussion_id till $discussionId
            $discussion_id = $discussionId;
            $user_id = $post["userid"];
            if (empty($user_id)) {
                $user_id = $fallbackUserId;
            } else {
                $checkUser = $flarumDbConnection->query("SELECT id FROM {$flarumDbPrefix}users WHERE id = '" . intval($user_id) . "'");
                if (!$checkUser || $checkUser->num_rows == 0) {
                    consoleOut("VARNING: Post $postId - user_id $user_id finns inte i Flarum. Kopplar till fallback-user $fallbackUserId.");
                    $user_id = $fallbackUserId;
                }
            }
            $created_at = date('Y-m-d H:i:s', $post['publishdate']);
            $is_approved = 1;
            $content = formatTextForFlarum($post['pagetext'] ?: $post['rawtext']);
            // Kör processAttachments innan content stoppas in i databasen
            [$content, $relativePaths] = processAttachments($vbulletinDbConnection, $flarumDbConnection, $postId, $content, $vbulletinDbPrefix, $flarumDbPrefix);
            // --- NYTT: Hämta och migrera bilder som är barn-noder (photo/image) ---
            $photoContentTypeId = 39; // Byt till rätt id om det är annorlunda i din databas
            $photoNodesQuery = $vbulletinDbConnection->query("SELECT nodeid FROM ${vbulletinDbPrefix}node WHERE parentid = '$postId' AND contenttypeid = $photoContentTypeId");
            if ($photoNodesQuery && $photoNodesQuery->num_rows > 0) {
                while ($photoNode = $photoNodesQuery->fetch_assoc()) {
                    $photoNodeId = $photoNode['nodeid'];
                    // Hämta filedataid från vb5_photo
                    $photoInfoQuery = $vbulletinDbConnection->query("SELECT filedataid, caption FROM ${vbulletinDbPrefix}photo WHERE nodeid = '$photoNodeId'");
                    if ($photoInfoQuery && $photoInfoQuery->num_rows > 0) {
                        $photoInfo = $photoInfoQuery->fetch_assoc();
                        $filedataid = $photoInfo['filedataid'];
                        $caption = $photoInfo['caption'];
                        // Hämta filinfo från vb5_filedata
                        $filedataQuery = $vbulletinDbConnection->query("SELECT * FROM ${vbulletinDbPrefix}filedata WHERE filedataid = '$filedataid'");
                        if ($filedataQuery && $filedataQuery->num_rows > 0) {
                            $filedata = $filedataQuery->fetch_assoc();
                            $filename = isset($filedata['filename']) && $filedata['filename'] ? $filedata['filename'] : ($filedataid . '.' . $filedata['extension']);
                            $extension = $filedata['extension'];
                            $filesize = $filedata['filesize'];
                            // --- Rekursiv sökning efter filen på disk ---
                            $foundPath = false;
                            $iterator = new RecursiveIteratorIterator(
                                new RecursiveDirectoryIterator($vbAttachmentPath, RecursiveDirectoryIterator::SKIP_DOTS),
                                RecursiveIteratorIterator::LEAVES_ONLY
                            );
                            foreach ($iterator as $file) {
                                if ($file->getFilename() === $filedataid . '.attach') {
                                    $sourcePath = $file->getPathname();
                                    $foundPath = true;
                                    break;
                                }
                            }
                            if (!$foundPath) {
                                consoleOut("BILDFIL SAKNAS: $filedataid.attach (filedataid=$filedataid, filename=$filename)");
                                continue;
                            }
                            $dateFolder = date('Y-m-d');
                            $destDir = $flarumUploadPath . '/' . $dateFolder;
                            if (!is_dir($destDir)) {
                                if (!mkdir($destDir, 0777, true)) {
                                    consoleOut("Kunde INTE skapa katalog: $destDir");
                                    continue;
                                }
                            }
                            $destFilename = time() . '-' . mt_rand(100000,999999) . '-' . preg_replace('/[^a-zA-Z0-9._-]/', '', $filename);
                            $destPath = $destDir . '/' . $destFilename;
                            $relativePath = $dateFolder . '/' . $destFilename;
                            $baseUrl = 'https://forum.ciklid.org';
                            $fullUrl = $baseUrl . '/assets/files/' . $relativePath;
                            if (!file_exists($destPath)) {
                                if (!copy($sourcePath, $destPath)) {
                                    consoleOut("Kunde INTE kopiera bild: $sourcePath -> $destPath");
                                    continue;
                                }
                            }
                            // Lägg in i fof_upload_files
                            $actorId = $user_id ?: $fallbackUserId;
                            $fileCheck = $flarumDbConnection->query("SELECT id FROM ${flarumDbPrefix}fof_upload_files WHERE path = '" . $flarumDbConnection->real_escape_string($relativePath) . "'");
                            if ($fileCheck && $fileCheck->num_rows > 0) {
                                $fileData = $fileCheck->fetch_assoc();
                                $flarumFileId = $fileData['id'];
                            } else {
                                $now = date('Y-m-d H:i:s');
                                $mimeType = mime_content_type($destPath);
                                $insertFile = $flarumDbConnection->query(
                                    "INSERT INTO ${flarumDbPrefix}fof_upload_files (actor_id, base_name, path, url, type, size, upload_method, created_at) VALUES (" .
                                    "'" . $actorId . "'," .
                                    "'" . $flarumDbConnection->real_escape_string($filename) . "'," .
                                    "'" . $flarumDbConnection->real_escape_string($relativePath) . "'," .
                                    "'" . $flarumDbConnection->real_escape_string($fullUrl) . "'," .
                                    "'" . $flarumDbConnection->real_escape_string($mimeType) . "'," .
                                    "'$filesize'," .
                                    "'local'," .
                                    "'$now')"
                                );
                                $flarumFileId = $flarumDbConnection->insert_id;
                            }
                            // Koppla filen till posten
                            $postExistsCheck = $flarumDbConnection->query("SELECT id FROM ${flarumDbPrefix}posts WHERE id = '$postId'");
                            if ($flarumFileId && $postExistsCheck && $postExistsCheck->num_rows > 0) {
                                $filePostCheck = $flarumDbConnection->query("SELECT * FROM ${flarumDbPrefix}fof_upload_file_posts WHERE file_id = '$flarumFileId' AND post_id = '$postId'");
                                if (!$filePostCheck || $filePostCheck->num_rows == 0) {
                                    $flarumDbConnection->query("INSERT INTO ${flarumDbPrefix}fof_upload_file_posts (file_id, post_id) VALUES ('$flarumFileId', '$postId')");
                                }
                            }
                            // Lägg till bild i postens innehåll
                            $content .= '<br><img src="' . $fullUrl . '" alt="' . htmlspecialchars($caption ?: $filename) . '" />';
                        }
                    }
                }
            }
            // JOIN node for parentid
            $attachQuery = $vbulletinDbConnection->query("SELECT a.*, n.* FROM ${vbulletinDbPrefix}attach a LEFT JOIN ${vbulletinDbPrefix}node n ON a.nodeid = n.nodeid WHERE n.parentid = '$postId' AND n.contenttypeid = '$attachContentTypeId' AND a.visible = 1");
            $found = 0;
            if ($attachQuery && $attachQuery->num_rows > 0) {
                consoleOut("Hittade " . $attachQuery->num_rows . " bilagor för post nodeid $postId");
                while ($attach = $attachQuery->fetch_assoc()) {
                    $found++;
                    consoleOut("Bilaga: nodeid=" . $attach['nodeid'] . ", filedataid=" . $attach['filedataid'] . ", filename=" . $attach['filename']);
                    $filename = $attach['filename'];
                    $filedataid = $attach['filedataid'];
                    $sourcePath = $vbAttachmentPath . '/' . $filedataid . '.attach';
                    $dateFolder = date('Y-m-d');
                    $destDir = $flarumUploadPath . '/' . $dateFolder;
                    if (!is_dir($destDir)) {
                        if (!mkdir($destDir, 0777, true)) {
                           consoleOut("Kunde INTE skapa katalog: $destDir");
                           continue;
                        } else {
                           consoleOut("Skapade katalog: $destDir");
                        }
                    }
                    $destFilename = time() . '-' . mt_rand(100000,999999) . '-' . preg_replace('/[^a-zA-Z0-9._-]/', '', $filename);
                    $destPath = $destDir . '/' . $destFilename;
                    $relativePath = $dateFolder . '/' . $destFilename;
                    // Bygg full URL för url-fältet
                    $baseUrl = 'https://forum.ciklid.org'; // Ändra till din domän om det behövs
                    $fullUrl = $baseUrl . '/assets/files/' . $relativePath;
                    $attachmentUrl = '/assets/files/' . $relativePath;

                    if (!file_exists($destPath) && file_exists($sourcePath)) {
                        copy($sourcePath, $destPath);
                    }
                    if (file_exists($destPath)) {
                        consoleOut("Filen kopierad: $destPath");
                    } else {
                        consoleOut("Kunde INTE kopiera filen: $sourcePath -> $destPath");
                    }

                    $actorId = $user_id;
                    if (empty($actorId)) $actorId = $fallbackUserId;
                    $fileCheck = $flarumDbConnection->query("SELECT id FROM ${flarumDbPrefix}fof_upload_files WHERE path = '" . $flarumDbConnection->real_escape_string($relativePath) . "'");
                    if ($fileCheck && $fileCheck->num_rows > 0) {
                        $fileData = $fileCheck->fetch_assoc();
                        $flarumFileId = $fileData['id'];
                    } else if (file_exists($destPath)) {
                        $now = date('Y-m-d H:i:s');
                        $mimeType = mime_content_type($destPath);
                        $filesize = filesize($destPath);
                        consoleOut("Försöker INSERT i fof_upload_files: $relativePath, $attachmentUrl, $actorId");
                        $insertFile = $flarumDbConnection->query(
                            "INSERT INTO ${flarumDbPrefix}fof_upload_files (actor_id, base_name, path, url, type, size, upload_method, created_at) VALUES (" .
                            "'" . $actorId . "'," .
                            "'" . $flarumDbConnection->real_escape_string($filename) . "'," .
                            "'" . $flarumDbConnection->real_escape_string($relativePath) . "'," .
                            "'" . $flarumDbConnection->real_escape_string($fullUrl) . "'," .
                            "'" . $flarumDbConnection->real_escape_string($mimeType) . "'," .
                            "'$filesize'," .
                            "'local'," .
                            "'$now')"
                        );
                        if ($insertFile === false) {
                            consoleOut("SQL error vid INSERT i fof_upload_files: " . $flarumDbConnection->error);
                        }
                        $flarumFileId = $flarumDbConnection->insert_id;
                    } else {
                        $flarumFileId = null;
                    }

                    if ($flarumFileId) {
                        $filePostCheck = $flarumDbConnection->query("SELECT * FROM ${flarumDbPrefix}fof_upload_file_posts WHERE file_id = '$flarumFileId' AND post_id = '$postId'");
                        if (!$filePostCheck || $filePostCheck->num_rows == 0) {
                            $flarumDbConnection->query("INSERT INTO ${flarumDbPrefix}fof_upload_file_posts (file_id, post_id) VALUES ('$flarumFileId', '$postId')");
                        }
                    }

                    if (strpos($content, $filename) === false && strpos($content, $fullUrl) === false) {
                        $content .= '<br><img src="' . $fullUrl . '" alt="' . htmlspecialchars($filename) . '" />';
                    }
                }
            }
            if ($found == 0) {
                consoleOut("Inga bilagor hittades för post nodeid $postId");
            }
            if (strpos($content, '<t>') !== 0) {
                $content = '<t>' . $content . '</t>';
            }
            $query = "INSERT INTO " . $flarumDbPrefix . "posts (
                id, discussion_id, number, created_at, user_id, type, content, is_approved, is_private
            ) VALUES (
                '$postId', '$discussion_id', $postNumber, '$created_at', '$user_id', 'comment', '$content', $is_approved, 0
            )";
            $res = $flarumDbConnection->query($query);
            if ($res === false) {
                consoleOut("SQL error creating post $postId");
                consoleOut($query, false);
                consoleOut($flarumDbConnection->error . "\n", false);
                $postNumber++;
                continue;
            }
            $migratedPosts++;
            $existingPostIds[$postId] = true;
            $postNumber++;
        }
        consoleOut("Tråd $discussionId: $migratedPosts inlägg migrerade, $skippedPosts inlägg hoppades över.");
        if ($curTopicCount % 50 == 0) consoleOut("Migrerat $curTopicCount av $questionCount trådar...");
    }
    consoleOut("\nBatch summering: $migratedDiscussions diskussioner migrerade, $skippedDiscussions diskussioner hoppades över.");
} else {
    consoleOut("Inga trådar hittades.");
}
}

//----------------------------------------------------------------------------- 
// User/Discussions record creation
//----------------------------------------------------------------------------- 
$step++;
if (!empty($runSteps[5])) {
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: USER/DISCUSSIONS RECORD CREATION\n", false);
$discussionQuery = $flarumDbConnection->query("SELECT id, user_id FROM ".$flarumDbPrefix."discussions");
$discussionCount = $discussionQuery ? $discussionQuery->num_rows : 0;
if ($discussionCount) {
    $created = 0;
    while($discussion = $discussionQuery->fetch_assoc()) {
        $userID = $discussion["user_id"];
        $discussionID = $discussion["id"];
        if ($userID === null || $userID === '' || $userID === 'NULL') {
            consoleOut("SKIP: discussion_user-relation för discussion_id $discussionID saknar user_id");
            continue;
        }
        if ($discussionID === null || $discussionID === '' || $discussionID === 'NULL') {
            consoleOut("SKIP: discussion_user-relation saknar discussion_id");
            continue;
        }
        $query = "INSERT IGNORE INTO ".$flarumDbPrefix."discussion_user (user_id, discussion_id) VALUES ( $userID, $discussionID)";
        $res = $flarumDbConnection->query($query);
        if($res === false) {
            consoleOut("SQL error vid discussion_user");
            consoleOut($query, false);
            consoleOut($flarumDbConnection->error."\n", false);
        } else {
            $created++;
        }
    }
    consoleOut("$created discussion_user-relationer skapade.");
} else {
    consoleOut("Inga diskussioner hittades.");
}
}

//----------------------------------------------------------------------------- 
// User discussion and comment count creation
//----------------------------------------------------------------------------- 
$step++;
if (!empty($runSteps[6])) {
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: EFTERBEARBETNING AV COUNTS OCH METADATA\n", false);
// comment_count
$syncQuery = "UPDATE {$flarumDbPrefix}discussions d SET comment_count = (SELECT COUNT(*) FROM {$flarumDbPrefix}posts p WHERE p.discussion_id = d.id)";
$flarumDbConnection->query($syncQuery);
// participant_count
$syncParticipantQuery = "UPDATE {$flarumDbPrefix}discussions d SET participant_count = (SELECT COUNT(DISTINCT user_id) FROM {$flarumDbPrefix}posts p WHERE p.discussion_id = d.id AND p.user_id IS NOT NULL)";
$flarumDbConnection->query($syncParticipantQuery);
// first/last_post_id
$syncFirstLastQuery = "UPDATE {$flarumDbPrefix}discussions d JOIN (SELECT discussion_id, MIN(id) AS first_post_id, MAX(id) AS last_post_id FROM {$flarumDbPrefix}posts GROUP BY discussion_id) p ON d.id = p.discussion_id SET d.first_post_id = p.first_post_id, d.last_post_id = p.last_post_id";
$flarumDbConnection->query($syncFirstLastQuery);
// last_posted_at
$syncLastPostedAtQuery = "UPDATE {$flarumDbPrefix}discussions d JOIN (SELECT discussion_id, MAX(created_at) AS last_posted_at FROM {$flarumDbPrefix}posts GROUP BY discussion_id) p ON d.id = p.discussion_id SET d.last_posted_at = p.last_posted_at";
$flarumDbConnection->query($syncLastPostedAtQuery);
// last_posted_user_id
$syncLastPostedUserQuery = "UPDATE {$flarumDbPrefix}discussions d JOIN (SELECT p1.discussion_id, p1.user_id FROM {$flarumDbPrefix}posts p1 JOIN (SELECT discussion_id, MAX(created_at) AS max_created FROM {$flarumDbPrefix}posts GROUP BY discussion_id) p2 ON p1.discussion_id = p2.discussion_id AND p1.created_at = p2.max_created) p ON d.id = p.discussion_id SET d.last_posted_user_id = p.user_id";
$flarumDbConnection->query($syncLastPostedUserQuery);
// discussion_count i tags
$syncTagCountQuery = "UPDATE {$flarumDbPrefix}tags t SET discussion_count = (SELECT COUNT(*) FROM {$flarumDbPrefix}discussion_tag dt WHERE dt.tag_id = t.id)";
$flarumDbConnection->query($syncTagCountQuery);
consoleOut("Efterbearbetning klar.");
}

//----------------------------------------------------------------------------- 
// Tag sort
//----------------------------------------------------------------------------- 
$step++;
if (!empty($runSteps[7])) {
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: TAG SORT\n", false);
$result = $flarumDbConnection->query("SELECT * FROM ".$flarumDbPrefix."tags ORDER BY position ASC, name ASC");
if ($result && $result->num_rows > 0) {
    $position = 0;
    $updated = 0;
    while ($row = $result->fetch_assoc()) {
        $query = "UPDATE ".$flarumDbPrefix."tags SET position = $position WHERE id = ".$row['id'].";";
        $res = $flarumDbConnection->query($query);
        if($res === false) {
            consoleOut("SQL error vid tag sort");
            consoleOut($query, false);
            consoleOut($flarumDbConnection->error."\n", false);
        } else {
            $updated++;
        }
        $position++;
    }
    consoleOut("$updated taggar sorterade.");
} else {
    consoleOut("Inga Flarum-taggar hittades.");
}
}

//----------------------------------------------------------------------------- 
// Wrapping it up
//----------------------------------------------------------------------------- 
$step++;
if (!empty($runSteps[8])) {
consoleOut("\n-------------------------------------------------------------------------------", false);
consoleOut("STEP $step: CLOSING DATABASE CONNECTIONS\n", false);
$flarumDbConnection->query("SET FOREIGN_KEY_CHECKS=1");
consoleOut("Foreign key checks re-enabled");

// Flytta hit loggningen av antal poster innan anslutningarna stängs
$res = $flarumDbConnection->query("SELECT COUNT(*) AS cnt FROM {$flarumDbPrefix}posts");
$row = $res->fetch_assoc();
consoleOut("Totalt antal inlägg i Flarum efter migrering: " . $row['cnt']);
$res = $vbulletinDbConnection->query("SELECT COUNT(*) AS cnt FROM {$vbulletinDbPrefix}node WHERE contenttypeid = 36");
$row = $res->fetch_assoc();
consoleOut("Totalt antal inlägg i vBulletin: " . $row['cnt']);

consoleOut("Klar!\nKontrollera din Flarum-installation.", false);

} // Avslutar if (!empty($runSteps[8]))

// =====================
// SLUTKONTROLLER OCH INDEXUPPDATERINGAR
// =====================
consoleOut("\nSLUTKONTROLLER OCH INDEXUPPDATERINGAR...\n", false);

// Kontroll: Diskussioner utan tagg
$res = $flarumDbConnection->query("SELECT d.id FROM {$flarumDbPrefix}discussions d LEFT JOIN {$flarumDbPrefix}discussion_tag dt ON d.id = dt.discussion_id WHERE dt.tag_id IS NULL");
if ($res && $res->num_rows > 0) {
    consoleOut("VARNING: Följande discussions saknar tagg:");
    while ($row = $res->fetch_assoc()) {
        consoleOut("Discussion ID: ".$row['id']);
    }
}

// Kontroll: Discussions utan first/last_post_id
$res = $flarumDbConnection->query("SELECT id FROM {$flarumDbPrefix}discussions WHERE first_post_id IS NULL OR last_post_id IS NULL");
if ($res && $res->num_rows > 0) {
    consoleOut("VARNING: Följande discussions saknar first/last_post_id:");
    while ($row = $res->fetch_assoc()) {
        consoleOut("Discussion ID: ".$row['id']);
    }
}

// Kontroll: Posts utan giltig discussion_id
$res = $flarumDbConnection->query("SELECT id FROM {$flarumDbPrefix}posts WHERE discussion_id IS NULL");
if ($res && $res->num_rows > 0) {
    consoleOut("VARNING: Följande posts saknar discussion_id:");
    while ($row = $res->fetch_assoc()) {
        consoleOut("Post ID: ".$row['id']);
    }
}

// Kontroll: Posts/discussions utan giltig user_id
$res = $flarumDbConnection->query("SELECT id FROM {$flarumDbPrefix}posts WHERE user_id NOT IN (SELECT id FROM {$flarumDbPrefix}users)");
if ($res && $res->num_rows > 0) {
    consoleOut("VARNING: Följande posts har ogiltig user_id:");
    while ($row = $res->fetch_assoc()) {
        consoleOut("Post ID: ".$row['id']);
    }
}
$res = $flarumDbConnection->query("SELECT id FROM {$flarumDbPrefix}discussions WHERE user_id NOT IN (SELECT id FROM {$flarumDbPrefix}users)");
if ($res && $res->num_rows > 0) {
    consoleOut("VARNING: Följande discussions har ogiltig user_id:");
    while ($row = $res->fetch_assoc()) {
        consoleOut("Discussion ID: ".$row['id']);
    }
}

// Synka index/counters igen
consoleOut("\nSynkar index och counters en extra gång...\n", false);
$flarumDbConnection->query("UPDATE {$flarumDbPrefix}discussions d SET comment_count = (SELECT COUNT(*) FROM {$flarumDbPrefix}posts p WHERE p.discussion_id = d.id)");
$flarumDbConnection->query("UPDATE {$flarumDbPrefix}discussions d SET participant_count = (SELECT COUNT(DISTINCT user_id) FROM {$flarumDbPrefix}posts p WHERE p.discussion_id = d.id AND p.user_id IS NOT NULL)");
$flarumDbConnection->query("UPDATE {$flarumDbPrefix}discussions d JOIN (SELECT discussion_id, MIN(id) AS first_post_id, MAX(id) AS last_post_id FROM {$flarumDbPrefix}posts GROUP BY discussion_id) p ON d.id = p.discussion_id SET d.first_post_id = p.first_post_id, d.last_post_id = p.last_post_id");
$flarumDbConnection->query("UPDATE {$flarumDbPrefix}discussions d JOIN (SELECT discussion_id, MAX(created_at) AS last_posted_at FROM {$flarumDbPrefix}posts GROUP BY discussion_id) p ON d.id = p.discussion_id SET d.last_posted_at = p.last_posted_at");
$flarumDbConnection->query("UPDATE {$flarumDbPrefix}discussions d JOIN (SELECT p1.discussion_id, p1.user_id FROM {$flarumDbPrefix}posts p1 JOIN (SELECT discussion_id, MAX(created_at) AS max_created FROM {$flarumDbPrefix}posts GROUP BY discussion_id) p2 ON p1.discussion_id = p2.discussion_id AND p1.created_at = p2.max_created) p ON d.id = p.discussion_id SET d.last_posted_user_id = p.user_id");
$flarumDbConnection->query("UPDATE {$flarumDbPrefix}tags t SET discussion_count = (SELECT COUNT(*) FROM {$flarumDbPrefix}discussion_tag dt WHERE dt.tag_id = t.id)");

consoleOut("\nSlutkontroller och indexuppdateringar klara!\n", false);
consoleOut("\nTips: Kör 'php flarum cache:clear' i Flarum-mappen efter migrering för att rensa cache och se alla nya poster direkt.\n", false);

$vbulletinDbConnection->close();
$flarumDbConnection->close();

