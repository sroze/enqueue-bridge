<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Sam\Symfony\Bridge\EnqueueMessage;

use Interop\Queue\Exception;
use Interop\Queue\PsrContext;
use Symfony\Component\Message\Transport\SenderInterface;
use Symfony\Component\Message\Transport\Serialization\EncoderInterface;

/**
 * Symfony Message sender to bridge Php-Enqueue producers.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
class QueueInteropSender implements SenderInterface
{
    /**
     * @var EncoderInterface
     */
    private $messageEncoder;

    /**
     * @var PsrContext
     */
    private $context;

    /**
     * @var string
     */
    private $destinationName;

    /**
     * @var bool
     */
    private $isTopic;

    /**
     * @var float
     */
    private $deliveryDelay;

    /**
     * @var float
     */
    private $timeToLive;

    /**
     * @var int
     */
    private $priority;

    public function __construct(
        EncoderInterface $messageEncoder,
        PsrContext $context,
        string $destinationName,
        bool $isTopic = true,
        float $deliveryDelay = null,
        float $timeToLive = null,
        int $priority = null
    ) {
        $this->messageEncoder = $messageEncoder;
        $this->context = $context;

        $this->destinationName = $destinationName;
        $this->isTopic = $isTopic;
        $this->deliveryDelay = $deliveryDelay;
        $this->timeToLive = $timeToLive;
        $this->priority = $priority;
    }

    /**
     * {@inheritdoc}
     */
    public function send($message)
    {
        $encodedMessage = $this->messageEncoder->encode($message);

        $destination = $this->isTopic ?
            $this->context->createTopic($this->destinationName) :
            $this->context->createQueue($this->destinationName)
        ;

        $message = $this->context->createMessage(
            $encodedMessage['body'],
            $encodedMessage['properties'] ?? [],
            $encodedMessage['headers'] ?? []
        );

        $producer = $this->context->createProducer();

        if (null !== $this->deliveryDelay) {
            $producer->setDeliveryDelay($this->deliveryDelay);
        }
        if (null !== $this->priority) {
            $producer->setPriority($this->priority);
        }
        if (null !== $this->timeToLive) {
            $producer->setTimeToLive($this->timeToLive);
        }

        try {
            $producer->send($destination, $message);
        } catch (Exception $e) {
            throw new SendingMessageFailedException($e->getMessage(), null, $e);
        }
    }
}
