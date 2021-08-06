import com.typesafe.sbt.SbtGit.GitKeys
import com.typesafe.sbt.git.JGit
import mdoc.DocusaurusPlugin.autoImport.{ docusaurusCreateSite, docusaurusProjectName }
import org.eclipse.jgit.api.{ CreateBranchCommand, Git }
import org.eclipse.jgit.lib.{ CommitBuilder, Constants, ObjectId, PersonIdent, Repository }
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.transport.{ JschConfigSessionFactory, SshSessionFactory }
import org.eclipse.jgit.treewalk.TreeWalk
import sbt._

object DeployDocs {

  val deployDocs: TaskKey[Unit] = taskKey("Deploy website to git repo")
  val deployBranch: SettingKey[String] = settingKey("Branch to deploy site")
  val deployRepo: SettingKey[String] = settingKey("Git repo to deploy site")
  val deployMessage: SettingKey[String] = settingKey("Commit message template")

  SshSessionFactory.setInstance(new JschConfigSessionFactory())

  private def deployDocsImpl = Def.task {
    val log = Keys.streams.value.log
    val site = docusaurusCreateSite.value

    val repoDir = IO.createTemporaryDirectory

    log.info("!!!!!!!!! " + sys.env.get("DEPLOY_DOCS_REPO"))

    log.info(s"Cloning ${deployRepo.value} into $repoDir")
    val jGit = JGit.clone(deployRepo.value, repoDir)
    val git = jGit.porcelain
    val repo = jGit.repo

    log.info(s"Cloned to $repo")

    val parentId = if (jGit.remoteBranches.contains(s"origin/${deployBranch.value}")) {
      val ref = git
        .checkout()
        .setCreateBranch(true)
        .setName(deployBranch.value)
        .setStartPoint(s"origin/${deployBranch.value}")
        .setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.TRACK)
        .call()

      log.info(s"Found origin/${deployBranch.value} id ${ref.getObjectId}")
      Some(ref.getObjectId)
    } else {
      log.info(s"origin/${deployBranch.value} is not found")
      None
    }

    rmAll(git)

    IO.copyDirectory(site / docusaurusProjectName.value, repoDir)
    git.add().addFilepattern(".").call()

    val status = git.status().call()

    if (status.isClean) {
      log.info("No changes for the site...")
    } else {
      val commitInfo = GitKeys.gitHeadCommit.value.getOrElse("UNKNOWN COMMIT")
      val message = deployMessage.value
        .replace("%version%", Keys.version.value)
        .replace("%commit%", commitInfo)
      val id = commit(repo, message, deployBranch.value, parentId)
      log.info(s"Changes commited '$message', $id")
//      git.push().call()
    }
  }

  private def rmAll(git: Git): Unit = {
    val repo = git.getRepository
    val rw = new RevWalk(repo)
    val tree = rw.parseCommit(repo.findRef(Constants.HEAD).getObjectId).getTree

    val walk = new TreeWalk(repo)
    walk.setRecursive(true)
    walk.addTree(tree)

    val rm = git.rm()
    while (walk.next()) rm.addFilepattern(walk.getPathString)
    rm.call()
  }

  private def commit(repo: Repository, message: String, branchName: String, parent: Option[ObjectId]): ObjectId = {

    val inserter = repo.newObjectInserter()
    val index = repo.lockDirCache()

    val treeId = index.writeTree(inserter)

    val ident = new PersonIdent(repo)
    val cb = new CommitBuilder()
    cb.setAuthor(ident)
    cb.setCommitter(ident)
    cb.setTreeId(treeId)
    cb.setMessage(message)
    parent.foreach(cb.addParentId)
    val commitId = inserter.insert(cb)

    val fullRef = Constants.R_HEADS + branchName
    val upd = repo.getRefDatabase.newUpdate(fullRef, false)
    upd.setNewObjectId(commitId)
    upd.update()

    val headUpd = repo.getRefDatabase.newUpdate(Constants.HEAD, false)
    headUpd.link(fullRef)

    index.unlock()
    commitId
  }

  def deployDocsSettings: Seq[Setting[_]] = Seq(
    deployDocs := deployDocsImpl.value,
    deployRepo := sys.env.get("DEPLOY_DOCS_REPO") getOrElse GitKeys.gitReader.value.withGit(_.remoteOrigin),
    deployBranch := "gh-pages",
    deployMessage := "BG-0: Create site version %version% (%commit%)."
  )
}
