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
      <div class="row flex-xl-nowrap">
        <div class="col-md-3 col-xl-2 bd-sidebar">
          $partials/nav()$
        </div>
        <nav class="d-none d-xl-block col-xl-2 bd-toc" aria-label="secondary-navigation">
          $partials/toc()$
        </nav>
        <main class="col-md-9 col-xl-8 py-md-3 pl-md-5 bd-content" role="main">
          $page.content$
        </main>
      </div>
    </div>

    $partials/footer()$
  </body>
</html>
