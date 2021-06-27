import com.typesafe.sbt.SbtGit.GitKeys
import com.typesafe.sbt.git.JGit
import mdoc.DocusaurusPlugin.autoImport.{ docusaurusCreateSite, docusaurusProjectName }
import org.eclipse.jgit.api.CreateBranchCommand
import org.eclipse.jgit.lib.Constants
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.transport.{ JschConfigSessionFactory, SshSessionFactory }
import org.eclipse.jgit.treewalk.TreeWalk
import sbt._

import scala.sys.process.Process

object DeployDocs {

  val deployDocs: TaskKey[Unit] = taskKey("Deploy website to git repo")
  val deployBranch: SettingKey[String] = settingKey("Branch to deploy site")
  val deployRepo: SettingKey[String] = settingKey("Git repo to deploy site")

  SshSessionFactory.setInstance(new JschConfigSessionFactory())

  private def deployDocsImpl = Def.task {
    val log = Keys.streams.value.log
    val site = docusaurusCreateSite.value

    val repoDir = IO.createTemporaryDirectory

    val jGit = JGit.clone(deployRepo.value, repoDir)
    val git = jGit.porcelain
    val repo = jGit.repo

    log.info(s"Cloned to $repo")

    if (jGit.remoteBranches.contains(s"origin/${deployBranch.value}")) {
      git
        .checkout()
        .setCreateBranch(true)
        .setName(deployBranch.value)
        .setStartPoint(s"origin/${deployBranch.value}")
        .setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.TRACK)
        .call()
    } else {
      Process("git" :: "checkout" :: "--orphan" :: deployBranch.value :: Nil, repoDir).!
    }

    val rw = new RevWalk(repo)
    val tree = rw.parseCommit(repo.findRef(Constants.HEAD).getObjectId).getTree

    val walk = new TreeWalk(repo)
    walk.setRecursive(true)
    walk.addTree(tree)

    val rm = git.rm()
    while (walk.next()) rm.addFilepattern(walk.getPathString)
    rm.call()

    IO.copyDirectory(site / docusaurusProjectName.value, repoDir)

    git.add().addFilepattern(".").call()

    val status = git.status().call()

    if (status.isClean) {
      log.info("No changes for the site...")
    } else {
      jGit.porcelain.commit().setMessage(s"BG-0: Create site version ${Keys.version.value}").call()
      jGit.porcelain.push().call()
    }
  }

  def deployDocsSettings: Seq[Setting[_]] = Seq(
    deployDocs := deployDocsImpl.value,
    deployRepo := GitKeys.gitReader.value.withGit(_.remoteOrigin),
    deployBranch := "gh-pages"
  )
}
