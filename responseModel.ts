export default class ResponseModel<TDto extends {}> {
    statusCode: number;
    status: 'SUCCESS' | 'FAILED';
    method: 'GET' | 'POST' | 'PUT' | 'DELETE';
    message: string;
    data: TDto;
    socketId: string = null;
    requestId: string = null;

    constructor(
        statusCode: number,
        status: 'SUCCESS' | 'FAILED',
        method: 'GET' | 'POST' | 'PUT' | 'DELETE',
        message: string,
        data: TDto = null
    ) {
        this.statusCode = statusCode;
        this.status = status;
        this.method = method;
        this.message = message;
        this.data = data;
    }
}