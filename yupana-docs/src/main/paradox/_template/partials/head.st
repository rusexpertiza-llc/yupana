<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
<link href='https://fonts.googleapis.com/css?family=Lora:400,700,400italic,700italic' rel='stylesheet' type='text/css'>
<link href='https://fonts.googleapis.com/css?family=Open+Sans:300italic,400italic,600italic,700italic,800italic,400,300,600,700,800' rel='stylesheet' type='text/css'>

<link rel="stylesheet" href="$page.base$/assets/css/bootstrap.min.css">
<link rel="stylesheet" href="$page.base$/assets/css/fontawesome-all.min.css">
<link rel="stylesheet" href="$page.base$/assets/css/clean-blog.min.css">
<link rel="stylesheet" href="$page.base$/assets/css/prism.css">
<link rel="stylesheet" href="$page.base$/assets/css/main.css">

$ if (page.properties.("title")) $
<title>$page.properties.("title")$</title>
$ elseif (page.title) $
<title>$page.title$$ if (!page.home.active) $ · $page.home.title$$ endif $</title>
$ else $
<title>$page.home.title$</title>
$ endif $
