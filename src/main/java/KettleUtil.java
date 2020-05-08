import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.kdr.KettleDatabaseRepository;
import org.pentaho.di.repository.kdr.KettleDatabaseRepositoryMeta;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

import java.time.LocalDateTime;

/**
 * @author xiaofu
 * @version 1.0
 * @date 2020/5/8 10:36
 * @description TODO
 */
public class KettleUtil {
    public static void execLocalTrans(String path) {
        //String transName ="C:\\software\\data-integration\\task\\import.ktr";
        try {
            //初始化kettle环境
            KettleEnvironment.init();
            //直接执行本地转换
            TransMeta transformationMetaNative =new TransMeta(path);
            //创建ktr
            Trans trans = new Trans(transformationMetaNative);
            //执行ktr
            trans.execute(null);
            //等待执行完毕
            trans.waitUntilFinished();
            if(trans.getErrors()>0) {
            System.err.println("Transformation run Failure!");
        }
            else {
            System.out.println("Transformation run successfully!");
            }
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }
    public static void execLocalJob(String path){
        try {
            KettleEnvironment.init();
            //jobname 是Job脚本的路径及名称  
            JobMeta jobMeta = new JobMeta(path, null);
            Job job = new Job(null, jobMeta);
            //向Job 脚本传递参数，脚本中获取参数值：${参数名}  
            //job.setVariable(paraname, paravalue);  
            job.start();
            job.waitUntilFinished();
            if (job.getErrors() > 0) {
                throw new Exception("There are errors during job exception!(执行job发生异常)");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void execRemoteTrans(String name){
        String transName = "import";
        try {
            //初始化kettle环境
            KettleEnvironment.init();
            //创建资源库对象，此时的对象还是一个空对象
            KettleDatabaseRepository repository = new KettleDatabaseRepository();
            //创建资源库数据库对象，类似我们在spoon里面创建资源库
            DatabaseMeta dataMeta =
                    new DatabaseMeta("mysql", "MYSQL", "Native", "localhost",

                            "mydb", "3306", "root", "123456");
            //资源库元对象,名称参数，id参数，描述等可以随便定义
            KettleDatabaseRepositoryMeta kettleDatabaseMeta =
                    new KettleDatabaseRepositoryMeta("mysql", "mysql", "mysql", dataMeta);
            kettleDatabaseMeta.setConnection(dataMeta);
            //给资源库赋值
            repository.init(kettleDatabaseMeta);
            //连接资源库
            repository.connect("admin", "admin");
            //根据变量查找到模型所在的目录对象,此步骤很重要。
            RepositoryDirectoryInterface directory = repository.findDirectory("/");
            //创建ktr元对象--资源库中的转换
            TransMeta transformationMetaRep = ((Repository) repository).loadTransformation(transName, directory, null, true, null);
            //创建ktr
            Trans trans = new Trans(transformationMetaRep);
            //执行ktr
            trans.execute(null);
            //等待执行完毕
            trans.waitUntilFinished();
            if (trans.getErrors() > 0) {
                System.err.println("TransformationrunFailure!");
            } else {
                System.out.println("Transformationrunsuccessfully!");
            }
            System.out.println(LocalDateTime.now());
        }catch (KettleException e){
            e.printStackTrace();
        }
    }
    public static void execRomoteJob(String jobName){
        try{
            //初始化kettle环境
            KettleEnvironment.init();
            //创建资源库对象，此时的对象还是一个空对象
            KettleDatabaseRepository repository = new KettleDatabaseRepository();
            //创建资源库数据库对象，类似我们在spoon里面创建资源库
            DatabaseMeta dataMeta = new DatabaseMeta("mysql","MYSQL","Native","localhost",
                    "mydb","3306","root","123456");
            //资源库元对象,名称参数，id参数，描述等可以随便定义
            KettleDatabaseRepositoryMeta kettleDatabaseMeta=
            new KettleDatabaseRepositoryMeta("mysql","mysql",null,dataMeta);
            //kettleDatabaseMeta.setConnection(dataMeta);
            //给资源库赋值
            repository.init(kettleDatabaseMeta);
            //连接资源库
            repository.connect("admin","admin");
            //根据变量查找到模型所在的目录对象,此步骤很重要。
            RepositoryDirectoryInterface directory = repository.findDirectory("/");
            //创建kjb元对象--资源库中的作业
            JobMeta jobMetaRep=((Repository)repository).loadJob(jobName,directory,null,null);//loadTransformation(transName,directory,null,true,null);
            //JobMetajobMeta=newJobMeta(jobName,null); 
            //JobMetajobMetaRep=newJobMeta(jobName,repository);
            Job job = new Job(repository,jobMetaRep);
            job.start();
            job.waitUntilFinished();
            if(job.getErrors()>0){
                System.err.println("TransformationrunFailure!");
            }
            else {
                System.out.println("Transformationrunsuccessfully!");
            }
        }catch(KettleException e){
            e.printStackTrace();
        }
    }
}
