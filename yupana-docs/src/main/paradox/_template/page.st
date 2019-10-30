<!DOCTYPE html>
<html>
  <head>
    $partials/head()$
  </head>
  <body>
    $partials/scripts()$
    <header class="masthead">
      <div class="overlay"></div>
      <div class="container">
        <div class="row">
          <div class="col-lg-8 col-md-10 mx-auto">
            <div class="post-heading">
              <h1 class="title-small">$page.title$</h1>
              $ if (page.subtitle) $
              <h2 class="subheading">$ page.subtitle $</h2>
              $ endif $
            </div>
          </div>
        </div>
      </div>
    </header>

    <div class="container-fluid">
      <div class="row">
        <div class="col">
          $partials/nav()$
        </div>
        <div class="col-6">
          <div class="container">
            $page.content$
          </div>
        </div>
        <div class="col">
          $partials/toc()$
        </div>
      </div>
    </div>

    $partials/footer()$
  </body>
</html>
