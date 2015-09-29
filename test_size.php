<?php
function curl_get_file_size( $url ) {
  // Assume failure.
  $result = -1;

  $curl = curl_init( $url );

  // Issue a HEAD request and follow any redirects.
  curl_setopt( $curl, CURLOPT_NOBODY, true );
  curl_setopt( $curl, CURLOPT_HEADER, true );
  curl_setopt( $curl, CURLOPT_RETURNTRANSFER, true );
  curl_setopt( $curl, CURLOPT_FOLLOWLOCATION, true );
  curl_setopt( $curl, CURLOPT_USERAGENT, 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.0' );

  $data = curl_exec( $curl );
  curl_close( $curl );

  if( $data ) {
    $content_length = "unknown";
    $status = "unknown";

    if( preg_match( "/^HTTP\/1\.[01] (\d\d\d)/", $data, $matches ) ) {
      $status = (int)$matches[1];
    }

    if( preg_match( "/Content-Length: (\d+)/", $data, $matches ) ) {
      $content_length = (int)$matches[1];
    }

    // http://en.wikipedia.org/wiki/List_of_HTTP_status_codes
    if( $status == 200 || ($status > 300 && $status <= 308) ) {
      $result = $content_length;
    }
  }

  return $result;
}

$sum=0;
if (($handle = fopen("registry.csv", "r")) !== FALSE) {
    while (($data = fgetcsv($handle, 0, ";")) !== FALSE) {
        $url= $data[15];
				if (substr($url, 0, 9)=='opendata/') 
					$url = 'http://data.gov.ru/'.$url;
				$size = curl_get_file_size($url);
//				if ($size<1) 
					echo $size." $url\n";
				if ($size>0)
					$sum += $size;
				sleep(1);
    }
		echo "Total size: ".$sum;
    fclose($handle);
}

?>
