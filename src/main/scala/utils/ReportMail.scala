package utils

/**
  * Created by kalit_000 on 9/2/17.
  */
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.log4j.Logger;

object ReportMail {

  private var sendFrom:String =null;
  private var sendTo:String=null;
  private var messageSubject:String=null;
  private var messageContent:String=null;
  val logger=Logger.getLogger("MessageProducer")

  def sendMail(sendFrom:String,sendTo:String,messageSubject:String,messageContent:String):Unit ={
    val properties=System.getProperties
    properties.put("mail.smtp.host","host")
    val session=Session.getDefaultInstance(properties)
    val message=new MimeMessage(session)

    logger.info("Sending Mail to :" + sendTo)
    logger.info("Sending Mail From "+sendFrom)

    //set the from,to,subject,body text
    message.setFrom(new InternetAddress(sendFrom))
    message.setRecipients(Message.RecipientType.TO,sendTo)
    message.setSubject(messageSubject)
    message.setText(messageContent)
    message.setHeader("Content-Type","text/html")

    // And sent it
    Transport.send(message)
    logger.info("Report Mail sent.")









  }




}
