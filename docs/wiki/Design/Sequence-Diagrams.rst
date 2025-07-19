Report Consumer
---------------

.. mermaid::

   sequenceDiagram
       actor API_User as API User
       participant Report_Consumer as Report Consumer
       participant DDS

       opt Callback Registration
           API_User->>Report_Consumer: registerCallback("on_new_report", handler)
           activate Report_Consumer
           Report_Consumer->>DDS: add_listener("on_new_report", handler)
           activate DDS
           DDS-->>Report_Consumer: listenerRegistered()
           deactivate DDS
           Report_Consumer-->>API_User: registrationSuccessful()
           deactivate Report_Consumer
       end

       opt Startup
           API_User->>Report_Consumer: start()
           activate Report_Consumer
           Report_Consumer->>DDS: init_reader()
           activate DDS
           DDS-->>Report_Consumer: initComplete()
           deactivate DDS
           Report_Consumer-->>API_User: started()
           deactivate Report_Consumer
       end

       opt on_new_report Event
           DDS->>Report_Consumer: on_new_report(reportData)
           activate Report_Consumer
           Report_Consumer-->>API_User: handler(reportData)
           deactivate Report_Consumer
       end

       opt Other DDS Events
           DDS->>Report_Consumer: on_data_available()
           activate Report_Consumer
           Report_Consumer-->>API_User: dataAvailableNotification()
           deactivate Report_Consumer

           DDS->>Report_Consumer: on_requested_deadline_missed(info)
           activate Report_Consumer
           Report_Consumer-->>API_User: deadlineMissedAlert(info)
           deactivate Report_Consumer

           DDS->>Report_Consumer: on_liveliness_changed(status)
           activate Report_Consumer
           Report_Consumer-->>API_User: livelinessChanged(status)
           deactivate Report_Consumer

           DDS->>Report_Consumer: on_sample_lost(details)
           activate Report_Consumer
           Report_Consumer-->>API_User: sampleLostWarning(details)
           deactivate Report_Consumer
       end

       opt Shutdown
           API_User->>Report_Consumer: shutdown()
           activate Report_Consumer
           Report_Consumer->>DDS: remove_listener("on_new_report")
           activate DDS
           DDS-->>Report_Consumer: listenerRemoved()
           deactivate DDS
           Report_Consumer-->>API_User: shutdownComplete()
           deactivate Report_Consumer
       end

Report Provider
---------------

.. mermaid::

   sequenceDiagram
       actor API_User as API user
       participant Report_Provider as Report Provider
       participant DDS

       opt Create Report Instance
           API_User->>Report_Provider: createReportInstance("reportId")
           activate Report_Provider
           Report_Provider->>DDS: register_keyed_instance("reportId")
           activate DDS
           DDS-->>Report_Provider: instanceHandle
           deactivate DDS
           Report_Provider-->>API_User: instanceCreated(instanceHandle)
           deactivate Report_Provider
       end

       opt Startup
           API_User->>Report_Provider: start()
           activate Report_Provider
           Report_Provider->>DDS: init_writer()
           activate DDS
           DDS-->>Report_Provider: writerInitialized()
           deactivate DDS
           Report_Provider->>DDS: write(livelinessTopic, reportId, status="online")
           activate DDS
           DDS-->>Report_Provider: announcementAck()
           deactivate DDS
           Report_Provider-->>API_User: started()
           deactivate Report_Provider
       end

       opt Update Report Samples
           API_User->>Report_Provider: send(reportData)
           activate Report_Provider
           Report_Provider->>DDS: write(reportKey, reportData)
           deactivate Report_Provider
       end

       opt Finish Report
           API_User->>Report_Provider: finishReport()
           activate Report_Provider
           Report_Provider->>DDS: dispose_instance("reportId")
           deactivate Report_Provider
       end

       opt Shutdown
           API_User->>Report_Provider: shutdown()
           activate Report_Provider
           Report_Provider->>DDS: dispose_instance("reportId")
           deactivate Report_Provider
       end
