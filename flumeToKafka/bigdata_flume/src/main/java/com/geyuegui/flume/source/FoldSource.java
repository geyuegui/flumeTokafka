package com.geyuegui.flume.source;

import com.geyuegui.com.uitl.FormatYYYYMMDD;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.io.File.separator;

public class FoldSource extends AbstractSource implements Configurable, PollableSource {

    private static final Logger LOG = Logger.getLogger(FoldSource.class);

    private String folderDir;
    private String succDir;
    private String failDir;
    private  int filenum;
    private  List<File> listFile;
    private List<Event> eventList;


    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        //read file
        try {
            Thread.currentThread().sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            List<File> files = (List<File>) FileUtils.listFiles(new File(folderDir), new String[]{"txt"}, true);
            int fileCount=files.size();
            if(fileCount>filenum){
                listFile=files.subList(0,filenum);
            }else {
                listFile=files;
            }
            if(listFile.size()>0){
                for(File file:listFile){

                    String filename=file.getName();
                    String fileNameNew =succDir+FormatYYYYMMDD.currentTimeFormatYYYYMMDD()+separator+filename;
                    if(!new File(fileNameNew).exists()){

                        List<String> lines= FileUtils.readLines(file);
                        for(String line:lines){
                            Event event = new SimpleEvent();
                            LOG.info("============ Event event = new SimpleEvent()============");
                            Map headers=new HashMap<String,String>();
                            headers.put("filename:",filename);
                            LOG.info("============ Source header put filename:"+filename);
                            headers.put("AbstractFilename:",fileNameNew);
                            LOG.error("============ Source header put AbstractFilename:"+fileNameNew);
                            event.setHeaders(headers);
                            event.setBody(line.getBytes());
                            LOG.info("============ Sourceevent put data:"+line);
                            eventList.add(event);
//                            getChannelProcessor().processEvent(event);

                        }
                        FileUtils.moveToDirectory(file,new File(succDir+FormatYYYYMMDD.currentTimeFormatYYYYMMDD()),true);
                    }
                }
                getChannelProcessor().processEventBatch(eventList);
                LOG.info("pushed data success :"+eventList.size()+"success");
                eventList.clear();
//                getChannelProcessor().
            }
            status=Status.READY;
        }catch (Exception e){
            e.printStackTrace();
            status=Status.BACKOFF;
            LOG.error(null,e);
        }finally {

        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    @Override
    public void configure(Context context) {
        eventList = new ArrayList<>();
        folderDir=context.getString("dirs");
        succDir=context.getString("successfile");
        filenum=context.getInteger("filenum");
        failDir=context.getString("failfile");
        LOG.info("----------"+
                "folderDir:"+folderDir+","+"successfile:"+succDir+",fileNum:"+filenum);

    }
}
