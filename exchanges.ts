const Exchange = {
    Topics: [
        {
            TopicName: "EMPLOYEE_ADD",
            Publishers: ["API_GATEWAY_SERVICE"],
            Method: "POST",
            Subscribers: [
                {
                    Service: "IOT_SERVICE",
                    OnSuccessTopicsToPush: ["EMPLOYEE_ADDED"],
                    OnFailureTopicsToPush: ["ERROR_RECEIVER"],
                    QueueName: "EMPLOYEE_ADD-IOT_SERVICE"
                },
            ],
        },
        {
            TopicName: "EMPLOYEE_ADDED",
            Publishers: ["IOT_SERVICE"],
            Method: "UNKNOWN",
            Subscribers: [
                {
                    Service: "API_GATEWAY_SERVICE",
                    OnSuccessTopicsToPush: [],
                    OnFailureTopicsToPush: [],
                    QueueName: "EMPLOYEE_ADDED-API_GATEWAY_SERVICE"
                },
            ],
        },
        {
            TopicName: "EMPLOYEE_UPDATE",
            Publishers: ["API_GATEWAY_SERVICE"],
            Method: "POST",
            Subscribers: [
                {
                    Service: "IOT_SERVICE",
                    OnSuccessTopicsToPush: ["EMPLOYEE_UPDATED"],
                    OnFailureTopicsToPush: ["ERROR_RECEIVER"],
                    QueueName: "EMPLOYEE_UPDATE-IOT_SERVICE"
                },
            ],
        },
        {
            TopicName: "EMPLOYEE_UPDATED",
            Publishers: ["IOT_SERVICE"],
            Method: "UNKNOWN",
            Subscribers: [
                {
                    Service: "API_GATEWAY_SERVICE",
                    OnSuccessTopicsToPush: [],
                    OnFailureTopicsToPush: [],
                    QueueName: "EMPLOYEE_UPDATED-API_GATEWAY_SERVICE"
                },
            ],
        },
        {
            TopicName: "EMPLOYEE_DELETE",
            Publishers: ["API_GATEWAY_SERVICE"],
            Method: "PUT",
            Subscribers: [
                {
                    Service: "IOT_SERVICE",
                    OnSuccessTopicsToPush: ["EMPLOYEE_DELETED"],
                    OnFailureTopicsToPush: ["ERROR_RECEIVER"],
                    QueueName: "EMPLOYEE_DELETE-IOT_SERVICE"
                },
            ],
        },
        {
            TopicName: "EMPLOYEE_DELETED",
            Publishers: ["IOT_SERVICE"],
            Method: "UNKNOWN",
            Subscribers: [
                {
                    Service: "API_GATEWAY_SERVICE",
                    OnSuccessTopicsToPush: [],
                    OnFailureTopicsToPush: [],
                    QueueName: "EMPLOYEE_DELETED-API_GATEWAY_SERVICE"
                },
            ],
        },
        {
            TopicName: "DEPARTMENT_ADD",
            Publishers: ["API_GATEWAY_SERVICE"],
            Method: "POST",
            Subscribers: [
                {
                    Service: "IOT_SERVICE",
                    OnSuccessTopicsToPush: ["DEPARTMENT_ADDED"],
                    OnFailureTopicsToPush: ["ERROR_RECEIVER"],
                    QueueName: "DEPARTMENT_ADD-IOT_SERVICE"
                },
            ],
        },
        {
            TopicName: "DEPARTMENT_ADDED",
            Publishers: ["IOT_SERVICE"],
            Method: "UNKNOWN",
            Subscribers: [
                {
                    Service: "API_GATEWAY_SERVICE",
                    OnSuccessTopicsToPush: [],
                    OnFailureTopicsToPush: [],
                    QueueName: "DEPARTMENT_ADDED-API_GATEWAY_SERVICE"
                },
            ],
        },
        {
            TopicName: "DEPARTMENT_UPDATE",
            Publishers: ["API_GATEWAY_SERVICE"],
            Method: "POST",
            Subscribers: [
                {
                    Service: "IOT_SERVICE",
                    OnSuccessTopicsToPush: ["DEPARTMENT_UPDATED"],
                    OnFailureTopicsToPush: ["ERROR_RECEIVER"],
                    QueueName: "DEPARTMENT_UPDATE-IOT_SERVICE"
                },
            ],
        },
        {
            TopicName: "DEPARTMENT_UPDATED",
            Publishers: ["IOT_SERVICE"],
            Method: "UNKNOWN",
            Subscribers: [
                {
                    Service: "API_GATEWAY_SERVICE",
                    OnSuccessTopicsToPush: [],
                    OnFailureTopicsToPush: [],
                    QueueName: "DEPARTMENT_UPDATED-API_GATEWAY_SERVICE"
                },
            ],
        },
        {
            TopicName: "DEPARTMENT_DELETE",
            Publishers: ["API_GATEWAY_SERVICE"],
            Method: "PUT",
            Subscribers: [
                {
                    Service: "IOT_SERVICE",
                    OnSuccessTopicsToPush: ["DEPARTMENT_DELETED"],
                    OnFailureTopicsToPush: ["ERROR_RECEIVER"],
                    QueueName: "DEPARTMENT_DELETE-IOT_SERVICE"
                },
            ],
        },
        {
            TopicName: "DEPARTMENT_DELETED",
            Publishers: ["IOT_SERVICE"],
            Method: "UNKNOWN",
            Subscribers: [
                {
                    Service: "API_GATEWAY_SERVICE",
                    OnSuccessTopicsToPush: [],
                    OnFailureTopicsToPush: [],
                    QueueName: "DEPARTMENT_DELETED-API_GATEWAY_SERVICE"
                },
            ],
        },
        {
            TopicName: "ERROR_RECEIVER",
            Publishers: ["IOT_SERVICE"],
            Method: "UNKNOWN",
            Subscribers: [
                {
                    Service: "API_GATEWAY_SERVICE",
                    OnSuccessTopicsToPush: [],
                    OnFailureTopicsToPush: [],
                    QueueName: "ERROR_RECEIVER-API_GATEWAY_SERVICE"
                },
            ],
        },
    ]
}

export default Exchange;
