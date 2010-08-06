;;; redis.rkt
;;;
;;; Implements an interface to the redis persistent key-value
;;; database. Communicates with the database through its TCP
;;; interface.
#lang racket

(define current-connection (make-parameter #f))

(define-struct connection (in out cust))

(define (connect)
  (let ([cust (make-custodian)])
    (parameterize ([current-custodian cust])
      (let-values ([(in out) (tcp-connect "localhost" 6379)])
        (make-connection in out cust)))))

(define (disconnect! conn)
  (with-handlers ([exn:fail:network? void])
    (close-output-port (connection-out conn)))
  (with-handlers ([exn:fail:network? void])
    (close-input-port (connection-in conn))))

(define (send-command command conn)
  (let ([out (connection-out conn)])
    (fprintf out "~a\r\n" command)
    (flush-output out)))

(define-struct exn:redis (message))

(define (read-response in)
  (match (read-bytes 1 in)
    [#"-" (read-line in 'return-linefeed)]
    [#"+" (read-line in 'return-linefeed)]
    [#"$" (read-bulk-response in)]
    [#"*" (read-multi-bulk-response in)]
    [#":" (string->number (read-line in 'return-linefeed))]
    [_ (raise (make-exn:redis (format "invalid control character: ~a"
                                      (read-byte in))))]))

(define (read-bulk-response in)
  (let ([length (string->number (read-line in 'return-linefeed))])
    (begin0 (read-bytes length in)
      (read-line in 'return-linefeed))))

(define (read-multi-bulk-response in)
  (build-list (string->number (read-line in 'return-linefeed))
              (lambda (_) (read-bulk-response in))))
    

(define (ping)
  (send-command "PING" (current-connection))
  (read-response (connection-in (current-connection))))

